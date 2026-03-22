(function () {
  function isoToUnixSeconds(value) {
    if (!value) return null;
    const parsed = Date.parse(value);
    if (Number.isNaN(parsed)) return null;
    return Math.floor(parsed / 1000);
  }

  function renderPanel(panel) {
    const chartRoot = panel.querySelector("[data-role='chart-root']");
    const dataNode = panel.querySelector("[data-role='bar-data']");
    if (!chartRoot || !dataNode || !window.LightweightCharts) {
      return;
    }

    if (chartRoot.__chartInstance) {
      chartRoot.__chartInstance.remove();
      chartRoot.__chartInstance = null;
    }

    const bars = JSON.parse(dataNode.textContent || "[]");
    const chart = window.LightweightCharts.createChart(chartRoot, {
      layout: {
        background: { color: "#fffaf1" },
        textColor: "#1b2430",
      },
      grid: {
        vertLines: { color: "rgba(223, 214, 197, 0.5)" },
        horzLines: { color: "rgba(223, 214, 197, 0.5)" },
      },
      width: chartRoot.clientWidth,
      height: chartRoot.clientHeight || 420,
      rightPriceScale: {
        borderColor: "#d8cfbf",
      },
      timeScale: {
        borderColor: "#d8cfbf",
        timeVisible: true,
        secondsVisible: false,
      },
    });

    const candleSeries = chart.addCandlestickSeries({
      upColor: "#0b8a63",
      downColor: "#b5473c",
      borderVisible: false,
      wickUpColor: "#0b8a63",
      wickDownColor: "#b5473c",
    });
    candleSeries.setData(
      bars.map((bar) => ({
        time: isoToUnixSeconds(bar.time),
        open: bar.open,
        high: bar.high,
        low: bar.low,
        close: bar.close,
      })),
    );

    const amountSeries = chart.addHistogramSeries({
      color: "rgba(199, 104, 57, 0.45)",
      priceFormat: { type: "volume" },
      priceScaleId: "",
    });
    amountSeries.priceScale().applyOptions({
      scaleMargins: {
        top: 0.78,
        bottom: 0,
      },
    });
    amountSeries.setData(
      bars.map((bar) => ({
        time: isoToUnixSeconds(bar.time),
        value: bar.amount || 0,
        color: bar.close >= bar.open ? "rgba(11, 138, 99, 0.4)" : "rgba(181, 71, 60, 0.4)",
      })),
    );

    chart.timeScale().fitContent();
    chartRoot.__chartInstance = chart;

    const resizeObserver = new ResizeObserver(() => {
      chart.applyOptions({ width: chartRoot.clientWidth });
    });
    resizeObserver.observe(chartRoot);
    chartRoot.__chartResizeObserver = resizeObserver;
  }

  function renderAll(scope) {
    scope.querySelectorAll("[data-role='instrument-panel']").forEach(renderPanel);
  }

  document.addEventListener("DOMContentLoaded", function () {
    renderAll(document);
  });

  document.body.addEventListener("htmx:afterSwap", function (event) {
    const target = event.target;
    if (!(target instanceof HTMLElement)) return;
    if (target.matches("[data-role='instrument-panel']")) {
      renderPanel(target);
      return;
    }
    renderAll(target);
  });
})();
