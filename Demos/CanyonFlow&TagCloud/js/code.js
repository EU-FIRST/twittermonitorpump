var chart;
var MIN_DATE 
	= Date.UTC(2012, 0, 1); // Jan 1, 2012
var MAX_DATE 
	= Date.UTC(2012, 11, 31); // Dec 31, 2012
var DAY_SPAN
	= 86400000;
var FONT
	= "12px 'Helvetica Neue',Helvetica,Arial,sans-serif";

/*function filter(data) {
	var newData = [];
	for (var i in data) {
		if (data[i][0] >= MIN_DATE && data[i][0] <= MAX_DATE + DAY_SPAN / 3 * 2) {
			var d = new Date(data[i][0]);
			d = Date.UTC(d.getFullYear(), d.getMonth(), d.getDate()); 
			newData.push([d, data[i][1]]); 
		}
	}
	return newData;
}

function align(data, refData) {
	var newData = [];
	var v = null;
	for (var i in refData) {
		if (data[refData[i][0]]) {
			newData.push([refData[i][0], v = data[refData[i][0]]]);
		} else {
			newData.push([refData[i][0], v]);
		}
	}
	return newData;
}

function MA(data, days) {
	var maData = [];
	var queue = [];
	var sum = 0;
	for (var i in data) {
		if (queue.length < days) {
			queue.push(data[i].y); sum += data[i].y; 
			maData.push(sum / queue.length);
		} else {
			queue.push(data[i].y); sum += data[i].y;
			sum -= queue.shift();
			maData.push(sum / days);
		}
	}
	return maData;
}*/

function error() {
	$(".loading-img").hide(); $(".loading-oops").show();
}

function load(name) {
	$(".loading-curtain,.loading-img").show();
	$("#datepicker-from").data("datetimepicker").setDate(MIN_DATE);
	$("#datepicker-to").data("datetimepicker").setDate(MAX_DATE);
	$("#topics-5").button("toggle");
	$("#all").button("toggle");

	$.getJSON("data/" + name + ".txt", function(data) {
		var series = [];
		var vol = [];
		var pointStart = Date.UTC(2012, 0, 1);
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
				pointStart: pointStart,
				pointInterval: pointInterval,
				type: "area",
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
						$("#datepicker-from").data("datetimepicker").setDate(event.min);
						$("#datepicker-to").data("datetimepicker").setDate(event.max);
						if (Math.abs(event.max - event.min - chart.span) >= 2 * DAY_SPAN) {
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
					text: "Canyon flow",
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
					//stacking: "normal"
				}
			},
			series: series 
				/*
			tooltip: {
				formatter: function() {
					var tooltip = Highcharts.dateFormat("%a, %b %d, %Y", this.x) + 
						"<br/><span style=\"color:" + this.points[0].series.color + "\">" + this.points[0].series.name + "</span>: <b>" + this.points[0].y + "</b>" +
						"<br/><span style=\"color:" + this.points[1].series.color + "\">Sentiment</span>: <b>" + this.points[1].y.toFixed(2) + "</b>";
					if (this.points[2]) {
						tooltip += "<br/><span style=\"color:" + this.points[2].series.color + "\">Sentiment MA</span>: <b>" + this.points[2].y.toFixed(2) + "</b>";
					}
					return tooltip;
				}
			}*/
		});
		chart.span = MAX_DATE - MIN_DATE;
		$(".loading-curtain,.loading-img").hide();
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

// assign zoom button handlers
$("#zoom .btn").click(function() {
	var action = $(this).attr("id");
	var span;
	var dateEnd = chart.xAxis[0].getExtremes().max;
	if (action == "all") { chart.xAxis[0].setExtremes(MIN_DATE, MAX_DATE); return; }
	else if (action == "1m") { span = 30 * DAY_SPAN; }
	else if (action == "3m") { span = 90 * DAY_SPAN; }
	else if (action == "6m") { span = 180 * DAY_SPAN; }
	var dateStart = dateEnd - span;
	if (dateStart < MIN_DATE) { dateEnd += (MIN_DATE - dateStart); dateStart += (MIN_DATE - dateStart); }
	chart.xAxis[0].setExtremes(dateStart, dateEnd);
});

// assign MA button handlers
/*$("#lower-chart .btn").click(function() {
	var action = $(this).attr("id");
	if (action == "none") {
		for (var i in chart.series[MA_IDX].data) { chart.series[MA_IDX].data[i].update(chart.series[SENT_IDX].data[i].y, false); } 
		chart.redraw();
		chart.series[MA_IDX].setVisible(false, true);
	}
	else { 
		var data = MA(chart.series[SENT_IDX].data, action == "7-day-avg" ? 7 : 14);
		for (var i in chart.series[MA_IDX].data) { chart.series[MA_IDX].data[i].update(data[i], false); }
		chart.series[MA_IDX].setVisible(true, true);
	}
});*/

// assign upper-chart button handlers
/*$("#upper-chart .btn").click(function() {
	var action = $(this).attr("id");
	if (action == "price") {
		chart.series[VOL_IDX].setVisible(false, false);
		chart.yAxis[0].setOptions($.extend({}, chart.yAxis[0].options, { min: null, title: { text: "Stock price", style: { font: FONT, color: "#000" } } }));
		chart.yAxis[0].setTitle(); // wtf?!
		chart.series[PRICE_IDX].setVisible(true, true);
	} else {
		chart.series[PRICE_IDX].setVisible(false, false);
		chart.yAxis[0].setOptions($.extend({}, chart.yAxis[0].options, { min: 0, title: { text: "Occurrence", style: { font: FONT, color: "#000" } } }));
		chart.yAxis[0].setTitle(); // wtf?!
		chart.series[VOL_IDX].setVisible(true, true);
	}
});*/

// assign selection handler !!!!!
/*$("select").change(function() {
	$(this)[0].blur(); // rmv ugly focus rectangle
	$("select option:selected").each(function () {
		load($(this).attr("value"));
	});
});*/

// initialize
load("AAPL"); 