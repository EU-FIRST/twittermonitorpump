var chart;

var MIN_DATE 
	= Date.UTC(2012, 0, 1); 

function load(name, period, gran) {
	showLoading();
	$.getJSON("data/" + name + "_" + period + "_" + gran + ".txt", function(data) {
		var series = [];
		var vol = [];
		var pointStart = MIN_DATE;
		var pointInterval = 3600 * 1000 * 24;
		for (var i in data) {
			if (vol.length == 0) {
				vol = vol.concat(data[i].data);
			} else {
				for (var j in data[i].data) {
					vol[j] += data[i].data[j];
				}
			}
		}
		series.push({
			visible: false,
			name: "vol",
			data: vol,
			pointStart: pointStart,
			pointInterval: pointInterval
		});
		for (var i in data) {
			series.push({
				name: data[i].name,
				data: data[i].data,
				keywords: data[i].keywords,
				staticKeywords: data[i].staticKeywords,
				pointStart: pointStart,
				pointInterval: pointInterval,
				type: "area",
				color: data[i].color,
				states: { hover: { enabled: false } },
				lineWidth: 1
			});
		}
		for (var i in data) {
			series.push({
				name: data[i].name + "_vol",
				data: data[i].data,
				pointStart: pointStart,
				pointInterval: pointInterval,
				color: data[i].color,
				states: { hover: { enabled: false } },
				lineWidth: 1,
				yAxis: 1
			});
		}
		chart = new Highcharts.StockChart({
			credits: { 
				enabled: false 
			},
			chart: {
				renderTo: "chart-container",
				zoomType: "x",
			},
			rangeSelector: { 
				enabled: false
			},
			navigator: {
				xAxis: {
					labels: {
						style: {
							font: FONT,
							color: "#000"
						}
					}
				}
			},
			xAxis: {			
				lineColor: "silver",
				tickColor: "silver",
				labels: {
					style: {
						font: FONT,
						color: "#000"
					}
				}
			},
			yAxis: [{
				title: {
					style: {
						font: FONT,
						color: "#000"
					},
					text: "Topic flow",
					x: -15
				},
				maxPadding: 0,
				min: 0,
				labels: {
					enabled: false,
				},
				plotLines: [{
					value: 0,
					width: 2,
					color: "silver"
				}],
				height: 250,
				lineWidth: 2,
				lineColor: "silver"
			},
			{
				title: {
					style: {
						font: FONT,
						color: "#000"
					},
					text: "Tweet count"
				},
				maxPadding: 0,
				min: 0,
				alignTicks: false,
				labels: {
					align: "right",
					style: {
						font: FONT,
						color: "#000"
					},
					x: -5,
				},
				plotLines:[{
					value: 0,
					width: 2,
					color: "silver"
				}],
				top: 280,
				height: 150,
				offset: 0,
				lineWidth: 2,
				lineColor: "silver"
			}],
			plotOptions: {
				area: {
					stacking: "percent"
				}
			},
			series: series,
			tooltip: {
				useHTML: true,
				formatter: function() {
					var staticKeywords = getOption("#keywords") == "keywords-static";
					var ptIdx = (this.x - MIN_DATE) / DAY_SPAN;
					var tooltip = "<span style=\"font-family:'Helvetica Neue',Helvetica,Arial,sans-serif\"><b>" + Highcharts.dateFormat("%a, %b %e, %Y", this.x) + "</b>";
					for (var i in this.points) {
						if (this.points[i].series.options.yAxis != 1 && this.points[i].y > 0) {
							var kwds = staticKeywords ? this.points[i].series.options.staticKeywords : this.points[i].series.options.keywords[ptIdx];
							if (kwds != null) {
								tooltip += "<br/><span style=\"color:" + this.points[i].series.color + "\">" + kwds + "</span> (" + Highcharts.numberFormat(this.points[i].y, 0) + ") <b>" + Highcharts.numberFormat(this.points[i].percentage, 0) + "%</b>";
							} 
						}
					}
					return tooltip + "</span>";
				}
			}
		});
		hideLoading();
	}).error(error);
}

// initialize buttons
$(".btn").focus(function() {
	$(this)[0].blur(); // fixes FF focus bug
});

// initialize selection box
$("#entity").val("AAPL");

// assign zoom reset handler 
$("#zoom .btn").click(function() {
	chart.xAxis[0].setExtremes();
});

// assign entity selection handler 
$("#entity").change(function() {
	$(this)[0].blur(); // rmv ugly focus rectangle
	load(getSelection("#entity"), getOption("#period"), getOption("#gran"));
});

// assign period selection handler
$("#period .btn").click(function() {
	var p = $(this).attr("id");
	if (p == "p1") { MIN_DATE = Date.UTC(2012, 0, 1); }
	else if (p == "p2") { MIN_DATE = Date.UTC(2012, 3, 1); }
	else if (p == "p3") { MIN_DATE = Date.UTC(2012, 6, 1); }
	else if (p == "p4") { MIN_DATE = Date.UTC(2012, 9, 1); }
	load(getSelection("#entity"), p, getOption("#gran"));
});

// assign granularity selection handler
$("#gran .btn").click(function() {
	var gran = $(this).attr("id");
	load(getSelection("#entity"), getOption("#period"), gran);
});

// initialize
load("AAPL", "p1", "coarse"); 