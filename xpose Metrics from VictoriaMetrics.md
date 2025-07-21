xpose Metrics from VictoriaMetrics
Ensure http://localhost:8428/metrics is accessible and shows vm_data_size_bytes types.

Configure Prometheus to Scrape VictoriaMetrics
Edit prometheus.yml to add VM scrape job and point rule_files to an alert rule file.

Create Alert Rule for Predictive Disk Usage
In alert.rules.yml, use predict_linear() on vm_data_size_bytes to forecast 2-day growth and alert at 90% threshold.

Restart Prometheus & Verify
Restart the Prometheus service, open http://localhost:9090, check Alerts tab and test forecast queries in the Graph tab.

Connect Prometheus to Grafana & Import Dashboard
In Grafana, add Prometheus as a data source, then create or import a dashboard with:

Actual usage

Forecasts (+1d, +2d)

80% & 90% disk threshold lines