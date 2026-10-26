package com.example.mergeddl

import android.net.ConnectivityManager
import android.net.Network
import android.net.NetworkCapabilities
import android.net.NetworkRequest
import android.os.Bundle
import android.os.Environment
import android.text.InputType
import android.view.ViewGroup
import android.widget.*
import androidx.appcompat.app.AppCompatActivity
import kotlinx.coroutines.*
import java.io.File
import java.io.RandomAccessFile
import java.net.HttpURLConnection
import java.net.URL
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.resume

/**
 * Downloads one file over Wi-Fi AND mobile data at the same time.
 * The file is cut into chunks (HTTP Range); each network pulls chunks from a
 * shared queue, so the faster link automatically carries more of the file.
 */
class MainActivity : AppCompatActivity() {

    private val scope = CoroutineScope(Dispatchers.Main + SupervisorJob())
    private lateinit var cm: ConnectivityManager
    private val callbacks = mutableListOf<ConnectivityManager.NetworkCallback>()
    private var job: Job? = null

    private lateinit var urlInput: EditText
    private lateinit var btn: Button
    private lateinit var bar: ProgressBar
    private lateinit var status: TextView

    private val CHUNK = 1L * 1024 * 1024      // 1 MB chunks
    private val THREADS_PER_NET = 3

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        cm = getSystemService(CONNECTIVITY_SERVICE) as ConnectivityManager

        val root = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setPadding(48, 96, 48, 48)
        }
        urlInput = EditText(this).apply {
            hint = "File URL (https://...)"
            inputType = InputType.TYPE_TEXT_VARIATION_URI
        }
        btn = Button(this).apply { text = "Download (Wi-Fi + SIM)" }
        bar = ProgressBar(this, null, android.R.attr.progressBarStyleHorizontal).apply { max = 1000 }
        status = TextView(this).apply { setPadding(0, 32, 0, 0) }
        listOf(urlInput, btn, bar, status).forEach {
            root.addView(it, LinearLayout.LayoutParams(
                ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.WRAP_CONTENT))
        }
        setContentView(root)

        btn.setOnClickListener {
            if (job?.isActive == true) { job?.cancel(); return@setOnClickListener }
            val url = urlInput.text.toString().trim()
            if (url.isEmpty()) { status.text = "Enter a URL"; return@setOnClickListener }
            btn.text = "Cancel"
            job = scope.launch {
                try { download(url) }
                catch (e: CancellationException) { status.text = "Cancelled" }
                catch (e: Exception) { status.text = "Error: ${e.message}" }
                finally { releaseNetworks(); btn.text = "Download (Wi-Fi + SIM)" }
            }
        }
    }

    override fun onDestroy() { scope.cancel(); releaseNetworks(); super.onDestroy() }

    // ---- Network acquisition -------------------------------------------------

    private suspend fun acquire(transport: Int): Network? = withTimeoutOrNull(8000) {
        suspendCancellableCoroutine<Network?> { cont ->
            val req = NetworkRequest.Builder()
                .addTransportType(transport)
                .addCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET)
                .build()
            val cb = object : ConnectivityManager.NetworkCallback() {
                override fun onAvailable(network: Network) { if (cont.isActive) cont.resume(network) }
            }
            callbacks += cb
            cm.requestNetwork(req, cb)
            cont.invokeOnCancellation { runCatching { cm.unregisterNetworkCallback(cb) } }
        }
    }

    private fun releaseNetworks() {
        callbacks.forEach { runCatching { cm.unregisterNetworkCallback(it) } }
        callbacks.clear()
    }

    private fun open(net: Network, url: String, range: String): HttpURLConnection =
        (net.openConnection(URL(url)) as HttpURLConnection).apply {
            connectTimeout = 10_000
            readTimeout = 15_000
            instanceFollowRedirects = true
            setRequestProperty("Range", range)
        }

    // ---- Download ------------------------------------------------------------

    private suspend fun download(url: String) {
        status.text = "Connecting to Wi-Fi and mobile data…"
        val wifi = acquire(NetworkCapabilities.TRANSPORT_WIFI)
        val cell = acquire(NetworkCapabilities.TRANSPORT_CELLULAR)
        val nets = listOfNotNull(wifi?.let { "Wi-Fi" to it }, cell?.let { "SIM" to it })
        if (nets.isEmpty()) error("No network available")
        if (nets.size == 1) status.text = "Only ${nets[0].first} available, using it alone"

        // Probe size + range support with a 1-byte ranged GET
        val total = withContext(Dispatchers.IO) {
            val c = open(nets[0].second, url, "bytes=0-0")
            try {
                if (c.responseCode != 206) -1L
                else c.getHeaderField("Content-Range")?.substringAfter('/')?.toLongOrNull() ?: -1L
            } finally { c.disconnect() }
        }
        if (total <= 0) error("Server doesn't support Range requests (needed for merging)")

        val name = url.substringBefore('?').substringAfterLast('/').ifEmpty { "download.bin" }
        val dir = getExternalFilesDir(Environment.DIRECTORY_DOWNLOADS)!!
        val file = File(dir, name)

        val queue = ConcurrentLinkedQueue<LongArray>() // [start, end, attempts]
        var s = 0L
        while (s < total) { queue.add(longArrayOf(s, minOf(s + CHUNK, total) - 1, 0)); s += CHUNK }

        val done = AtomicLong(0)
        val perNet = nets.associate { it.first to AtomicLong(0) }
        val failure = java.util.concurrent.atomic.AtomicReference<String?>(null)
        val workerScope = CoroutineScope(currentCoroutineContext() + Dispatchers.IO)

        val raf = RandomAccessFile(file, "rw").apply { setLength(total) }
        val channel = raf.channel
        val start = System.currentTimeMillis()

        val workers = run {
            nets.flatMap { (label, net) ->
                (1..THREADS_PER_NET).map {
                    workerScope.launch {
                        val buf = ByteArray(64 * 1024)
                        while (isActive && failure.get() == null) {
                            val chunk = queue.poll() ?: break
                            try {
                                val c = open(net, url, "bytes=${chunk[0]}-${chunk[1]}")
                                if (c.responseCode != 206) throw Exception("HTTP ${c.responseCode}")
                                var pos = chunk[0]
                                c.inputStream.use { input ->
                                    while (true) {
                                        val n = input.read(buf)
                                        if (n < 0) break
                                        channel.write(ByteBuffer.wrap(buf, 0, n), pos)
                                        pos += n
                                        done.addAndGet(n.toLong())
                                        perNet[label]!!.addAndGet(n.toLong())
                                    }
                                }
                                c.disconnect()
                            } catch (e: Exception) {
                                if (!isActive) break
                                if (chunk[2] < 3) { chunk[2]++; queue.add(chunk) }
                                else failure.set("Chunk failed on $label: ${e.message}")
                            }
                        }
                    }
                }
            }
        }

        // Progress UI
        var lastBytes = 0L; var lastTime = start
        try {
            while (workers.any { it.isActive }) {
                delay(500)
                val now = System.currentTimeMillis()
                val d = done.get()
                val speed = (d - lastBytes) * 1000.0 / (now - lastTime) / 1024 / 1024
                lastBytes = d; lastTime = now
                bar.progress = (d * 1000 / total).toInt()
                status.text = buildString {
                    append("%.1f / %.1f MB   %.2f MB/s\n".format(d / 1e6, total / 1e6, speed))
                    perNet.forEach { (k, v) -> append("$k: %.1f MB\n".format(v.get() / 1e6)) }
                }
            }
            workers.joinAll()
        } finally { channel.close(); raf.close() }

        failure.get()?.let { error(it) }
        val secs = (System.currentTimeMillis() - start) / 1000.0
        bar.progress = 1000
        status.text = "Done in %.1fs\nSaved: ${file.absolutePath}\n".format(secs) +
            perNet.entries.joinToString("\n") { "${it.key}: %.1f MB".format(it.value.get() / 1e6) }
    }
}
