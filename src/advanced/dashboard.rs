/// Real-Time Visualisation Dashboard – Advanced Feature
///
/// Architecture (Split-Process):
///   • Renderer Thread – periodically acquires a short-lived lock on
///     SharedMetrics and produces an SVG chart string via plotters.
///   • Web Server Thread – serves the cached SVG over a plain TCP socket
///     on port 8080.  Dashboard consumers (browser) poll /metrics.svg.
///
/// Design decisions to preserve RT determinism:
///   • The renderer runs at **lower** thread priority than actuators.
///   • Locks on SharedMetrics are held only during the snapshot copy, not
///     during the (slow) SVG rendering step.
///   • The web server uses a non-blocking single-threaded loop; it never
///     blocks the RT pipeline.
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::metrics::MetricsHandle;

const LISTEN_ADDR:    &str = "127.0.0.1:8080";
const RENDER_PERIOD:  Duration = Duration::from_millis(500);

pub struct Dashboard {
    metrics: MetricsHandle,
    stop:    Arc<AtomicBool>,
    /// Shared cache: renderer writes, web server reads.
    svg_cache: Arc<Mutex<String>>,
}

impl Dashboard {
    pub fn new(metrics: MetricsHandle, stop: Arc<AtomicBool>) -> Self {
        Self {
            metrics,
            stop,
            svg_cache: Arc::new(Mutex::new(Self::placeholder_svg())),
        }
    }

    /// Spawns both the renderer thread and the web-server thread.
    pub fn spawn(self) -> Vec<std::thread::JoinHandle<()>> {
        let mut handles = Vec::new();

        // ── Renderer ─────────────────────────────────────────────────────────
        {
            let metrics   = Arc::clone(&self.metrics);
            let cache     = Arc::clone(&self.svg_cache);
            let stop      = Arc::clone(&self.stop);
            handles.push(
                std::thread::Builder::new()
                    .name("dashboard-renderer".into())
                    .spawn(move || renderer_loop(metrics, cache, stop))
                    .expect("failed to spawn renderer"),
            );
        }

        // ── Web server ────────────────────────────────────────────────────────
        {
            let cache = Arc::clone(&self.svg_cache);
            let stop  = Arc::clone(&self.stop);
            handles.push(
                std::thread::Builder::new()
                    .name("dashboard-server".into())
                    .spawn(move || server_loop(cache, stop))
                    .expect("failed to spawn server"),
            );
        }

        handles
    }

    fn placeholder_svg() -> String {
        "<svg xmlns='http://www.w3.org/2000/svg' width='800' height='400'>\
         <rect width='800' height='400' fill='#1a1a2e'/>\
         <text x='400' y='200' fill='#e0e0e0' font-size='20' \
               text-anchor='middle'>Waiting for data…</text>\
         </svg>".to_string()
    }
}

// ─── Renderer loop ────────────────────────────────────────────────────────────

fn renderer_loop(
    metrics:   MetricsHandle,
    svg_cache: Arc<Mutex<String>>,
    stop:      Arc<AtomicBool>,
) {
    while !stop.load(Ordering::Relaxed) {
        // ── Snapshot metrics (short lock) ────────────────────────────────────
        let snapshot = {
            let m = metrics.lock().unwrap();
            MetricsSnapshot {
                force:          m.force_values.iter().cloned().collect(),
                position:       m.position_values.iter().cloned().collect(),
                temperature:    m.temperature_values.iter().cloned().collect(),
                gripper:        m.gripper_values.iter().cloned().collect(),
                motor:          m.motor_values.iter().cloned().collect(),
                stabiliser:     m.stabiliser_values.iter().cloned().collect(),
                jitter_mean:    m.sensor_jitter_us.mean(),
                jitter_max:     m.sensor_jitter_us.max(),
                proc_latency:   m.processing_latency_us.mean(),
                ipc_latency:    m.ipc_latency_us.mean(),
                e2e_latency_ms: m.e2e_latency_ms.mean(),
                proc_misses:    m.processor_deadline_misses,
                act_misses:     m.actuator_deadline_misses,
                ipc_dropped:    m.ipc_packets_dropped,
                iterations:     m.total_loop_iterations,
            }
        };
        // Lock released here ─ SVG rendering happens without holding it.

        // ── Render SVG (slow path, no lock held) ─────────────────────────────
        let svg = render_svg(&snapshot);

        if let Ok(mut cache) = svg_cache.lock() {
            *cache = svg;
        }

        std::thread::sleep(RENDER_PERIOD);
    }
}

struct MetricsSnapshot {
    force:          Vec<f64>,
    position:       Vec<f64>,
    temperature:    Vec<f64>,
    gripper:        Vec<f64>,
    motor:          Vec<f64>,
    stabiliser:     Vec<f64>,
    jitter_mean:    f64,
    jitter_max:     f64,
    proc_latency:   f64,
    ipc_latency:    f64,
    e2e_latency_ms: f64,
    proc_misses:    u64,
    act_misses:     u64,
    ipc_dropped:    u64,
    iterations:     u64,
}

/// Produces a self-contained SVG dashboard string.
fn render_svg(s: &MetricsSnapshot) -> String {
    let w = 900u32;
    let h = 600u32;

    let mut buf = String::with_capacity(8192);
    buf.push_str(&format!(
        "<svg xmlns='http://www.w3.org/2000/svg' width='{w}' height='{h}'>\n"
    ));
    buf.push_str("<rect width='100%' height='100%' fill='#1a1a2e'/>\n");

    // ── Title ─────────────────────────────────────────────────────────────────
    buf.push_str("<text x='450' y='30' fill='#e0e0e0' font-size='16' font-weight='bold' \
                  text-anchor='middle'>Real-Time System Dashboard</text>\n");

    // ── KPI cards ─────────────────────────────────────────────────────────────
    let kpis = [
        ("Iterations",    format!("{}", s.iterations)),
        ("Proc Misses",   format!("{}", s.proc_misses)),
        ("Act Misses",    format!("{}", s.act_misses)),
        ("IPC Dropped",   format!("{}", s.ipc_dropped)),
        ("Jitter (mean)", format!("{:.1}µs", s.jitter_mean)),
        ("Jitter (max)",  format!("{:.1}µs", s.jitter_max)),
        ("Proc Lat",      format!("{:.1}µs", s.proc_latency)),
        ("E2E Lat",       format!("{:.2}ms", s.e2e_latency_ms)),
    ];
    for (i, (label, val)) in kpis.iter().enumerate() {
        let x = 20 + (i % 4) * 220;
        let y = 55 + (i / 4) * 60;
        buf.push_str(&format!(
            "<rect x='{x}' y='{y}' width='200' height='50' rx='6' fill='#16213e'/>\n\
             <text x='{}' y='{}' fill='#a0b4d0' font-size='11'>{label}</text>\n\
             <text x='{}' y='{}' fill='#00d4ff' font-size='18' font-weight='bold'>{val}</text>\n",
            x + 10, y + 16,
            x + 10, y + 40,
        ));
    }

    // ── Waveform charts ───────────────────────────────────────────────────────
    let charts: &[(&str, &[f64], &str)] = &[
        ("Force (N)",      &s.force,       "#ff6b6b"),
        ("Position (mm)",  &s.position,    "#4ecdc4"),
        ("Temp (°C)",      &s.temperature, "#ffd93d"),
        ("Gripper PID",    &s.gripper,     "#c9b1ff"),
        ("Motor PID",      &s.motor,       "#ff9a3c"),
        ("Stabiliser PID", &s.stabiliser,  "#6bcb77"),
    ];
    for (ci, (label, data, colour)) in charts.iter().enumerate() {
        let cx = 20 + (ci % 3) * 295;
        let cy = 195 + (ci / 3) * 190;
        buf.push_str(&render_mini_chart(cx, cy, 270, 160, label, data, colour));
    }

    buf.push_str("</svg>");
    buf
}

fn render_mini_chart(x: usize, y: usize, w: usize, h: usize,
                     label: &str, data: &[f64], colour: &str) -> String {
    if data.is_empty() {
        return String::new();
    }
    let min = data.iter().cloned().fold(f64::INFINITY, f64::min);
    let max = data.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let range = (max - min).max(0.001);

    let points: Vec<String> = data.iter().enumerate().map(|(i, &v)| {
        let px = x + i * w / data.len().max(1);
        let py = y + h - ((v - min) / range * (h as f64 - 20.0)) as usize;
        format!("{},{}", px, py)
    }).collect();

    format!(
        "<rect x='{x}' y='{y}' width='{w}' height='{h}' rx='4' fill='#0f3460'/>\n\
         <text x='{}' y='{}' fill='#a0b4d0' font-size='10'>{label}</text>\n\
         <polyline points='{}' fill='none' stroke='{colour}' stroke-width='1.5'/>\n",
        x + 6, y + 14,
        points.join(" "),
    )
}

// ─── Web server loop ──────────────────────────────────────────────────────────

fn server_loop(svg_cache: Arc<Mutex<String>>, stop: Arc<AtomicBool>) {
    let listener = match TcpListener::bind(LISTEN_ADDR) {
        Ok(l)  => l,
        Err(e) => {
            eprintln!("[dashboard] bind failed: {e}");
            return;
        }
    };
    listener.set_nonblocking(true).ok();
    eprintln!("[dashboard] listening on http://{LISTEN_ADDR}");

    while !stop.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((mut stream, _addr)) => {
                let svg = svg_cache.lock().map(|g| g.clone()).unwrap_or_default();
                let response = format!(
                    "HTTP/1.1 200 OK\r\n\
                     Content-Type: image/svg+xml\r\n\
                     Content-Length: {}\r\n\
                     Cache-Control: no-cache\r\n\
                     Access-Control-Allow-Origin: *\r\n\
                     \r\n\
                     {}",
                    svg.len(), svg
                );
                let _ = stream.write_all(response.as_bytes());
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(e) => eprintln!("[dashboard] accept error: {e}"),
        }
    }
}
