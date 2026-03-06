<?php

$servername = getenv('MYSQLHOST') ?: "localhost";
$username = getenv('MYSQLUSER') ?: "baza_nomi";
$password = getenv('MYSQLPASSWORD') ?: "baza_paroli";
$database = getenv('MYSQLDATABASE') ?: $username;
$port = getenv('MYSQLPORT') ?: "3306";

try {
  $connect = mysqli_connect($servername, $username, $password, $database, $port);
  if (!$connect) {
    throw new Exception(mysqli_connect_error());
  }
} catch (Exception $e) {
  if (function_exists('debug_log')) {
    debug_log("Database Connection Failed: " . $e->getMessage());
  }
  die("Database Connection Error");
}

mysqli_query($connect, "CREATE TABLE IF NOT EXISTS `user_id` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user_id` varchar(250) NOT NULL,
  `status` text NOT NULL,
  `refid` varchar(11) NOT NULL DEFAULT '0',
  `sana` varchar(250) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;");
mysqli_query($connect, "CREATE TABLE IF NOT EXISTS `status` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user_id` varchar(250) NOT NULL,
  `kun` varchar(250) NOT NULL,
  `date` text NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;");
mysqli_query($connect, "CREATE TABLE IF NOT EXISTS `send` (
  `send_id` int(11) NOT NULL,
  `time1` text CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `time2` text CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `start_id` text CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `stop_id` text CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `admin_id` text CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `message_id` text CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `reply_markup` text CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `step` text CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `time3` text NOT NULL,
  `time4` text NOT NULL,
  `time5` text NOT NULL,
  PRIMARY KEY(`send_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;");
mysqli_query($connect, "CREATE TABLE IF NOT EXISTS `kabinet` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user_id` varchar(250) NOT NULL,
  `pul` varchar(250) NOT NULL,
  `pul2` varchar(250) NOT NULL,
  `odam` varchar(250) NOT NULL,
  `ban` text NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;");
mysqli_query($connect, "CREATE TABLE IF NOT EXISTS `anime_datas` (
  `data_id` int(11) NOT NULL AUTO_INCREMENT,
  `id` text NOT NULL,
  `file_id` text NOT NULL,
  `qism` text NOT NULL,
  `sana` text DEFAULT NULL,
  PRIMARY KEY (`data_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;");
mysqli_query($connect, "CREATE TABLE IF NOT EXISTS `animelar` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `nom` text NOT NULL,
  `rams` text NOT NULL,
  `qismi` text NOT NULL,
  `davlat` text NOT NULL,
  `tili` text NOT NULL,
  `yili` text NOT NULL,
  `janri` text NOT NULL,
  `qidiruv` text NOT NULL,
  `sana` text NOT NULL,
  `aniType` text NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;");

mysqli_query($connect, "CREATE TABLE IF NOT EXISTS `bot` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user` varchar(250) NOT NULL,
  `holat` varchar(10) NOT NULL DEFAULT 'On',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;");

// Initialize bot row if missing
if (isset($bot)) {
  $check_bot = mysqli_query($connect, "SELECT * FROM bot WHERE user = '$bot'");
  if (mysqli_num_rows($check_bot) == 0) {
    mysqli_query($connect, "INSERT INTO bot (user, holat) VALUES ('$bot', 'On')");
  }
}
