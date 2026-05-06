/// Multi-Actuator – Component B
///
/// Three independent threads (Gripper, Motor, Stabiliser) run at the
/// maximum OS thread priority to eliminate head-of-line blocking:
/// a slow Gripper will never delay Motor or Stabiliser dispatch.
///
/// Each thread applies the PID control signal, enforces a 2 ms execution
/// deadline, and feeds results back to the Processor via a shared channel.
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam::channel::{Receiver, Sender};
use thread_priority::{set_current_thread_priority, ThreadPriority};

use crate::metrics::MetricsHandle;
use crate::types::{ActuatorType, ControlSignal};

const ACTUATOR_DEADLINE: Duration = Duration::from_millis(2);

/// Spawns three actuator threads, one per ActuatorType.
pub fn spawn_actuators(
    ctrl_rx:  Receiver<ControlSignal>,
    metrics:  MetricsHandle,
    stop:     Arc<AtomicBool>,
) -> Vec<std::thread::JoinHandle<()>> {
    // All three actuators share the same incoming signal channel.
    // Each drains packets destined for its own actuator type.
    let actuator_types = [
        ActuatorType::Gripper,
        ActuatorType::Motor,
        ActuatorType::Stabiliser,
    ];

    actuator_types
        .into_iter()
        .map(|at| {
            let rx_clone      = ctrl_rx.clone();
            let metrics_clone = Arc::clone(&metrics);
            let stop_clone    = Arc::clone(&stop);

            std::thread::Builder::new()
                .name(format!("actuator-{}", at))
                .spawn(move || {
                    // Elevate priority (best-effort; fails gracefully without root).
                    let _ = set_current_thread_priority(ThreadPriority::Max);
                    run_actuator(at, rx_clone, metrics_clone, stop_clone);
                })
                .expect("failed to spawn actuator thread")
        })
        .collect()
}

fn run_actuator(
    actuator_type: ActuatorType,
    ctrl_rx:       Receiver<ControlSignal>,
    metrics:       MetricsHandle,
    stop:          Arc<AtomicBool>,
) {
    while !stop.load(Ordering::Relaxed) {
        // Each actuator only processes signals destined for it.
        let signal = match ctrl_rx.recv_timeout(Duration::from_millis(10)) {
            Ok(s) if s.actuator_type == actuator_type => s,
            Ok(_)  => continue, // wrong actuator – put back? crossbeam doesn't support that;
                                 // the router in mod.rs uses per-actuator channels instead.
            Err(_) => continue,
        };

        let dispatch_start = Instant::now();

        // ── Simulate actuator mechanics ─────────────────────────────────────
        // Clamp the PID output to a safe physical range and apply it.
        let clamped_output = signal.output.clamp(-100.0, 100.0);
        let _ = simulate_actuation(actuator_type, clamped_output);

        let dispatch_ns = dispatch_start.elapsed().as_nanos() as f64;
        let deadline_miss = dispatch_start.elapsed() > ACTUATOR_DEADLINE;

        // ── Record metrics ──────────────────────────────────────────────────
        if let Ok(mut m) = metrics.try_lock() {
            m.actuator_dispatch_ns.push(dispatch_ns);
            if deadline_miss {
                m.actuator_deadline_misses += 1;
            }
            match actuator_type {
                ActuatorType::Gripper    => m.push_gripper(clamped_output),
                ActuatorType::Motor      => m.push_motor(clamped_output),
                ActuatorType::Stabiliser => m.push_stabiliser(clamped_output),
            }
        }
    }
}

/// Simulates the physical response of an actuator to a control signal.
/// In a real system this would write to hardware registers or send a CAN frame.
#[inline(never)]
fn simulate_actuation(actuator_type: ActuatorType, output: f64) -> f64 {
    // Black-box arithmetic so the compiler cannot optimise this away.
    let factor = match actuator_type {
        ActuatorType::Gripper    => 0.85,
        ActuatorType::Motor      => 0.92,
        ActuatorType::Stabiliser => 0.78,
    };
    output * factor
}

// ─── Per-actuator channel router ─────────────────────────────────────────────
// The controller dispatches to a single Sender<ControlSignal>.  The router
// below splits that into three per-actuator channels so each thread only
// processes its own signals – eliminating the skip-and-loop problem above.

pub struct ActuatorRouter {
    pub gripper_tx:    Sender<ControlSignal>,
    pub motor_tx:      Sender<ControlSignal>,
    pub stabiliser_tx: Sender<ControlSignal>,
}

impl ActuatorRouter {
    pub fn route(&self, signal: ControlSignal) {
        let tx = match signal.actuator_type {
            ActuatorType::Gripper    => &self.gripper_tx,
            ActuatorType::Motor      => &self.motor_tx,
            ActuatorType::Stabiliser => &self.stabiliser_tx,
        };
        let _ = tx.try_send(signal);
    }
}

/// Spawns three dedicated actuator threads (one channel each) and returns
/// the router that the Controller uses to dispatch signals.
pub fn spawn_routed_actuators(
    metrics: MetricsHandle,
    stop:    Arc<AtomicBool>,
) -> (ActuatorRouter, Vec<std::thread::JoinHandle<()>>) {
    use crossbeam::channel::bounded;

    let (g_tx, g_rx) = bounded::<ControlSignal>(32);
    let (m_tx, m_rx) = bounded::<ControlSignal>(32);
    let (s_tx, s_rx) = bounded::<ControlSignal>(32);

    let router = ActuatorRouter {
        gripper_tx:    g_tx,
        motor_tx:      m_tx,
        stabiliser_tx: s_tx,
    };

    let handles: Vec<_> = [
        (ActuatorType::Gripper,    g_rx),
        (ActuatorType::Motor,      m_rx),
        (ActuatorType::Stabiliser, s_rx),
    ]
    .into_iter()
    .map(|(at, rx)| {
        let m  = Arc::clone(&metrics);
        let st = Arc::clone(&stop);
        std::thread::Builder::new()
            .name(format!("actuator-{}", at))
            .spawn(move || {
                let _ = set_current_thread_priority(ThreadPriority::Max);
                run_actuator_dedicated(at, rx, m, st);
            })
            .expect("failed to spawn actuator thread")
    })
    .collect();

    (router, handles)
}

fn run_actuator_dedicated(
    actuator_type: ActuatorType,
    ctrl_rx:       Receiver<ControlSignal>,
    metrics:       MetricsHandle,
    stop:          Arc<AtomicBool>,
) {
    while !stop.load(Ordering::Relaxed) {
        let signal = match ctrl_rx.recv_timeout(Duration::from_millis(10)) {
            Ok(s)  => s,
            Err(_) => continue,
        };

        let dispatch_start = Instant::now();

        let clamped_output = signal.output.clamp(-100.0, 100.0);
        let _ = simulate_actuation(actuator_type, clamped_output);

        let dispatch_ns   = dispatch_start.elapsed().as_nanos() as f64;
        let deadline_miss = dispatch_start.elapsed() > ACTUATOR_DEADLINE;

        if let Ok(mut m) = metrics.try_lock() {
            m.actuator_dispatch_ns.push(dispatch_ns);
            if deadline_miss {
                m.actuator_deadline_misses += 1;
            }
            match actuator_type {
                ActuatorType::Gripper    => m.push_gripper(clamped_output),
                ActuatorType::Motor      => m.push_motor(clamped_output),
                ActuatorType::Stabiliser => m.push_stabiliser(clamped_output),
            }
        }
    }
}
