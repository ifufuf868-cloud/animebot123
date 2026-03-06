<?php
header("Content-Type: text/plain");
$log_file = __DIR__ . "/admin/bot_debug.log";

if (file_exists($log_file)) {
    echo "--- LAST 100 LOG ENTRIES ---\n\n";
    $lines = file($log_file);
    $last_lines = array_slice($lines, -100);
    echo implode("", $last_lines);
} else {
    echo "Log file not found at: " . $log_file;
}
