use std::io::Write;

// https://unix.stackexchange.com/questions/66197/how-to-cause-kernel-panic-with-a-single-command
// echo 1 > /proc/sys/kernel/sysrq
// echo c > /proc/sysrq-trigger
pub fn panic_kernel() {
    let f = File::open("/proc/sys/kernel/sysrq").unwrap();
    f.write_all(b"1").unwrap();
    let f = File::open("/proc/sysrq-trigger").unwrap();
    f.write_all(b"c").unwrap();
}
