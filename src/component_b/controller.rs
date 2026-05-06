/// Controller – PID-based actuation (Component B)
///
/// Each incoming SensorData packet triggers a PID computation.
/// The controller maps sensor types to virtual actuators:
///   Force       → Gripper
///   Position    → Motor
///   Temperature → Stabiliser
///
/// Execution deadline: 2 ms per packet.
/// Deadline misses are flagged and sent via the feedback channel.
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam::channel::{Receiver, Sender};
use pid::Pid;

use crate::metrics::MetricsHandle;
use crate::types::{ActuatorType, ControlSignal, FeedbackMessage, SensorData, SensorType};

const CTRL_DEADLINE: Duration = Duration::from_millis(2);

/// Per-actuator PID setpoints.
const FORCE_SETPOINT: f64       = 10.0; // Newtons
const POSITION_SETPOINT: f64    =  5.0; // mm
const TEMPERATURE_SETPOINT: f64 = 25.0; // °C

pub struct Controller {
    metrics:      MetricsHandle,
    force_pid:    Pid<f64>,
    pos_pid:      Pid<f64>,
    temp_pid:     Pid<f64>,
}

impl Controller {
    pub fn new(metrics: MetricsHandle) -> Self {
        // Kp, Ki, Kd chosen for a stable response on a simulated 5 ms loop.
        let make_pid = |setpoint: f64| {
            let mut p: Pid<f64> = Pid::new(setpoint, 100.0);
            p.p(1.2, 100.0).i(0.05, 50.0).d(0.01, 10.0);
            p
        };

        Self {
            metrics,
            force_pid: make_pid(FORCE_SETPOINT),
            pos_pid:   make_pid(POSITION_SETPOINT),
            temp_pid:  make_pid(TEMPERATURE_SETPOINT),
        }
    }

    pub fn spawn(
        mut self,
        ctrl_rx:     Receiver<SensorData>,
        actuator_tx: Sender<ControlSignal>,
        feedback_tx: Sender<FeedbackMessage>,
        stop:        Arc<AtomicBool>,
    ) -> std::thread::JoinHandle<()> {
        std::thread::Builder::new()
            .name("controller".into())
            .spawn(move || self.run(ctrl_rx, actuator_tx, feedback_tx, stop))
            .expect("failed to spawn controller thread")
    }

    fn run(
        &mut self,
        ctrl_rx:     Receiver<SensorData>,
        actuator_tx: Sender<ControlSignal>,
        feedback_tx: Sender<FeedbackMessage>,
        stop:        Arc<AtomicBool>,
    ) {
        while !stop.load(Ordering::Relaxed) {
            let pkt = match ctrl_rx.recv_timeout(Duration::from_millis(10)) {
                Ok(p)  => p,
                Err(_) => continue,
            };

            let ctrl_start = Instant::now();

            // ── PID computation ─────────────────────────────────────────────
            let (actuator_type, setpoint, pid_output) = match pkt.sensor_type {
                SensorType::Force => {
                    let out = self.force_pid.next_control_output(pkt.filtered_value).output;
                    (ActuatorType::Gripper, FORCE_SETPOINT, out)
                }
                SensorType::Position => {
                    let out = self.pos_pid.next_control_output(pkt.filtered_value).output;
                    (ActuatorType::Motor, POSITION_SETPOINT, out)
                }
                SensorType::Temperature => {
                    let out = self.temp_pid.next_control_output(pkt.filtered_value).output;
                    (ActuatorType::Stabiliser, TEMPERATURE_SETPOINT, out)
                }
            };

            let pid_latency_ns = ctrl_start.elapsed().as_nanos() as f64;

            // ── Deadline check ──────────────────────────────────────────────
            let deadline_miss = ctrl_start.elapsed() > CTRL_DEADLINE;

            // ── Record metrics ──────────────────────────────────────────────
            if let Ok(mut m) = self.metrics.try_lock() {
                m.pid_latency_ns.push(pid_latency_ns);
                if deadline_miss {
                    m.actuator_deadline_misses += 1;
                }
            }

            // ── Feedback to Processor ───────────────────────────────────────
            let fb_start = Instant::now();
            let _ = feedback_tx.try_send(FeedbackMessage {
                actuator_type,
                deadline_miss,
                timestamp: Instant::now(),
            });
            let fb_latency_ns = fb_start.elapsed().as_nanos() as f64;
            if let Ok(mut m) = self.metrics.try_lock() {
                m.feedback_latency_ns.push(fb_latency_ns);
            }

            // ── Dispatch control signal to Actuators ────────────────────────
            let signal = ControlSignal {
                actuator_type,
                setpoint,
                measurement: pkt.filtered_value,
                output: pid_output,
                timestamp: Instant::now(),
            };
            let _ = actuator_tx.try_send(signal);
        }
    }
}
