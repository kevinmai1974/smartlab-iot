CREATE DATABASE IF NOT EXISTS smartlab;
USE smartlab;

CREATE TABLE IF NOT EXISTS telemetry (
  id INT AUTO_INCREMENT PRIMARY KEY,
  device VARCHAR(64) NOT NULL,
  topic VARCHAR(255) NOT NULL,
  value DOUBLE NULL,
  unit VARCHAR(16) NULL,
  ts_utc VARCHAR(40) NOT NULL,
  INDEX idx_device_ts (device, ts_utc)
);

CREATE TABLE IF NOT EXISTS events (
  id INT AUTO_INCREMENT PRIMARY KEY,
  device VARCHAR(64) NOT NULL,
  topic VARCHAR(255) NOT NULL,
  payload TEXT NOT NULL,
  ts_utc VARCHAR(40) NOT NULL,
  INDEX idx_device_ts (device, ts_utc)
);
