(function (global) {
  function createStandardChart(container, options) {
    var chartOptions = options && options.chartOptions ? options.chartOptions : {};
    var seriesOptions = options && options.seriesOptions ? options.seriesOptions : {};
    var decimals = options && options.decimals ? options.decimals : 5;

    var chart = LightweightCharts.createChart(container, {
      layout: {
        background: { type: 'solid', color: '#fffaf2' },
        textColor: '#5b4b3a',
      },
      grid: {
        vertLines: { color: 'rgba(91, 75, 58, 0.08)' },
        horzLines: { color: 'rgba(91, 75, 58, 0.08)' },
      },
      crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
      rightPriceScale: { borderColor: 'rgba(91, 75, 58, 0.18)' },
      timeScale: {
        borderColor: 'rgba(91, 75, 58, 0.18)',
        timeVisible: true,
        secondsVisible: false,
      },
      ...chartOptions,
    });

    var candleSeries = chart.addCandlestickSeries({
      upColor: '#1f7a49',
      downColor: '#b23b29',
      borderUpColor: '#1f7a49',
      borderDownColor: '#b23b29',
      wickUpColor: '#1f7a49',
      wickDownColor: '#b23b29',
      priceFormat: {
        type: 'price',
        precision: decimals,
        minMove: 1 / Math.pow(10, decimals),
      },
      ...seriesOptions,
    });

    new ResizeObserver(function () {
      chart.applyOptions({ width: container.clientWidth, height: container.clientHeight });
    }).observe(container);

    return { chart: chart, candleSeries: candleSeries };
  }

  function addZoneBand(chart, lower, upper, fillColor, bars) {
    if (!chart || !lower || !upper || !bars || bars.length < 2) return null;
    var series = chart.addBaselineSeries({
      baseValue: { type: 'price', price: lower },
      topFillColor1: fillColor,
      topFillColor2: fillColor,
      topLineColor: 'transparent',
      bottomFillColor1: 'transparent',
      bottomFillColor2: 'transparent',
      bottomLineColor: 'transparent',
      lineWidth: 0,
      priceLineVisible: false,
      lastValueVisible: false,
      crosshairMarkerVisible: false,
    });

    var pad = 365 * 24 * 3600;
    var padStart = Number(bars[0].time);
    var padEnd = Number(bars[bars.length - 1].time);
    series.setData([
      { time: padStart - pad, value: upper },
      { time: padEnd + pad, value: upper },
    ]);
    return series;
  }

  function addZoneBands(chart, support, resistance, bars) {
    if (!chart || !bars || !bars.length) return [];
    var bands = [];
    if (support) {
      var supportSeries = addZoneBand(chart, support.lower, support.upper, 'rgba(31, 122, 73, 0.10)', bars);
      if (supportSeries) bands.push(supportSeries);
    }
    if (resistance) {
      var resistanceSeries = addZoneBand(chart, resistance.lower, resistance.upper, 'rgba(178, 59, 41, 0.10)', bars);
      if (resistanceSeries) bands.push(resistanceSeries);
    }
    return bands;
  }

  function addZoneLines(series, support, resistance, decimals) {
    if (!series) return [];
    var lines = [];
    if (support) {
      lines.push(series.createPriceLine({
        price: support.lower,
        color: '#1f7a49',
        lineWidth: 1,
        lineStyle: LightweightCharts.LineStyle.Dashed,
        axisLabelVisible: true,
        title: 'S ' + (support.strength || ''),
      }));
      lines.push(series.createPriceLine({
        price: support.upper,
        color: 'rgba(31, 122, 73, 0.4)',
        lineWidth: 1,
        lineStyle: LightweightCharts.LineStyle.Dotted,
        axisLabelVisible: false,
      }));
    }
    if (resistance) {
      lines.push(series.createPriceLine({
        price: resistance.upper,
        color: '#b23b29',
        lineWidth: 1,
        lineStyle: LightweightCharts.LineStyle.Dashed,
        axisLabelVisible: true,
        title: 'R ' + (resistance.strength || ''),
      }));
      lines.push(series.createPriceLine({
        price: resistance.lower,
        color: 'rgba(178, 59, 41, 0.4)',
        lineWidth: 1,
        lineStyle: LightweightCharts.LineStyle.Dotted,
        axisLabelVisible: false,
      }));
    }

    if (typeof decimals === 'number' && decimals >= 0 && lines.length && support || resistance) {
      // Keep behavior consistent with display formatting by forcing a repaint.
      // The chart library uses series price formatting.
    }

    return lines;
  }

  global.fxChartCore = {
    createStandardChart: createStandardChart,
    addZoneBand: addZoneBand,
    addZoneBands: addZoneBands,
    addZoneLines: addZoneLines,
    defaultDecimals: 5,
  };
})(window);
