var chart;
var MIN_DATE 
	= Date.UTC(2012, 0, 1); // Jan 1, 2012
var MAX_DATE 
	= Date.UTC(2012, 2, 31); // Mar 31, 2012

function load(name, period, gran) {
	showLoading();
	$("#datepicker-from").data("datetimepicker").setDate(MIN_DATE);
	$("#datepicker-to").data("datetimepicker").setDate(MAX_DATE);
	$("#all").button("toggle");
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
				events: {
					setExtremes: function(event) {
						$("#datepicker-from").data("datetimepicker").setDate(dateOnly(event.min));
						$("#datepicker-to").data("datetimepicker").setDate(dateOnly(event.max));
						if (Math.abs(event.max - event.min - chart.span) >= DAY_SPAN) {
							chart.span = event.max - event.min;
							$("#zoom button").removeClass("active");
						}
					}
				},
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
					text: "Occurrence"
				},
				maxPadding: 0,
				minPadding: 0,
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
				top: 300,
				height: 100,
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
					var staticKeywords = getOption("#keywords") == "static";
					var ptIdx = (this.x - MIN_DATE) / DAY_SPAN;
					var tooltip = "<span style=\"font-family:'Helvetica Neue',Helvetica,Arial,sans-serif\">" + Highcharts.dateFormat("%a, %b %e, %Y", this.x);
					for (var i in this.points) {
						if (this.points[i].series.options.yAxis != 1 && this.points[i].y > 0) {
							var kwds = staticKeywords ? this.points[i].series.options.staticKeywords : this.points[i].series.options.keywords[ptIdx];
							if (kwds != null) {
								tooltip += "<br/><span style=\"color:" + this.points[i].series.color + "\">" + kwds + "</span> (" + fmtNum(this.points[i].y, 0) + ")";
							} 
						}
					}
					return tooltip + "</span>";
				}
			}
		});
		chart.span = MAX_DATE - MIN_DATE;
		hideLoading();
	}).error(error);
}

// initialize buttons
$(".btn").focus(function() {
	$(this)[0].blur(); // fixes FF focus bug
});

// initialize selection box
$("#entity").val("AAPL");

// initialize date pickers
$("#datepicker-from,#datepicker-to").datetimepicker({
	pickTime: false
}).on("changeDate", function(e) {
	var dateStart = $("#datepicker-from").data("datetimepicker").getDate().getTime();
	var dateEnd = $("#datepicker-to").data("datetimepicker").getDate().getTime();
	if (dateEnd < dateStart) { dateEnd = dateStart; }
	dateStart = Math.max(MIN_DATE, Math.min(dateStart, MAX_DATE));
	dateEnd = Math.max(MIN_DATE, Math.min(dateEnd, MAX_DATE));
	chart.xAxis[0].setExtremes(dateStart, dateEnd);
});

// assign zoom selection handler 
$("#zoom .btn").click(function() {
	var action = $(this).attr("id");
	var span;
	var dateEnd = chart.xAxis[0].getExtremes().max;
	if (action == "all") { chart.xAxis[0].setExtremes(MIN_DATE, MAX_DATE); return; }
	else if (action == "1w") { span = 7 * DAY_SPAN; }
	else if (action == "2w") { span = 14 * DAY_SPAN; }
	else if (action == "1m") { span = 30 * DAY_SPAN; }
	var dateStart = dateEnd - span;
	if (dateStart < MIN_DATE) { dateEnd += (MIN_DATE - dateStart); dateStart += (MIN_DATE - dateStart); }
	chart.xAxis[0].setExtremes(dateStart, dateEnd);
});

// assign entity selection handler 
$("#entity").change(function() {
	$(this)[0].blur(); // rmv ugly focus rectangle
	load(getSelection("#entity"), getOption("#period"), getOption("#gran"));
});

// assign period selection handler
$("#period .btn").click(function() {
	var p = $(this).attr("id");
	if (p == "p1") { MIN_DATE = Date.UTC(2012, 0, 1); MAX_DATE = Date.UTC(2012, 2, 31); }
	else if (p == "p2") { MIN_DATE = Date.UTC(2012, 3, 1); MAX_DATE = Date.UTC(2012, 5, 30); }
	else if (p == "p3") { MIN_DATE = Date.UTC(2012, 6, 1); MAX_DATE = Date.UTC(2012, 8, 30); }
	else if (p == "p4") { MIN_DATE = Date.UTC(2012, 9, 1); MAX_DATE = Date.UTC(2012, 11, 31); }
	load(getSelection("#entity"), p, getOption("#gran"));
});

// assign granularity selection handler
$("#gran .btn").click(function() {
	var gran = $(this).attr("id");
	load(getSelection("#entity"), getOption("#period"), gran);
});

// initialize
load("AAPL", "p1", "coarse"); 