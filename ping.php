<?php
$token = "8764008152:AAFEJYwsOn-yKBzGnhshshWACi7EnP5qyGM";
$chat_id = "6526385624";
$text = "Ping from Railway! Time: " . date("H:i:s");

$url = "https://api.telegram.org/bot$token/sendMessage?chat_id=$chat_id&text=" . urlencode($text);
$res = file_get_contents($url);

echo "Response from Telegram: " . $res;
