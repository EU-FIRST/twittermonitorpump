var chart;
var MIN_DATE 
	= Date.UTC(2012, 0, 1); // Jan 1, 2012
var MAX_DATE 
	= Date.UTC(2012, 11, 31); // Dec 31, 2012
var DAY_SPAN
	= 86400000;
var FONT
	= "12px 'Helvetica Neue',Helvetica,Arial,sans-serif";
var VOL_IDX 
	= 0; 
var PRICE_IDX 
	= 1;
var SENT_IDX 
	= 2; 
var MA_IDX 
	= 3;

function dateOnly(ms) {
	var d = new Date(ms);
	return Date.UTC(d.getFullYear(), d.getMonth(), d.getDate());
}

function numFmt(num, d) {
	return Highcharts.numberFormat(num, d).replace("-", "−");
}

function filter(data) {
	var newData = [];
	for (var i in data) {
		if (data[i][0] >= MIN_DATE && data[i][0] <= MAX_DATE + DAY_SPAN / 3 * 2) {
			var d = dateOnly(data[i][0]);
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
}

function error() {
	$(".loading-img").hide(); $(".loading-oops").show();
}

function load(name) {
	// set initial time span
	$(".loading-curtain,.loading-img").show();
	$("#datepicker-from").data("datetimepicker").setDate(MIN_DATE);
	$("#datepicker-to").data("datetimepicker").setDate(MAX_DATE);
	$("#all").button("toggle");
	$("#none").button("toggle");
	$("#occurrence").button("toggle");
	$.getJSON("volume/" + name + ".txt", function(volume) {
		volume = filter(volume);
		$.getJSON("sentiment/" + name + ".txt", function(sentiment) {
			sentiment = filter(sentiment);
			$.getJSON("price/" + name + ".txt", function(price) {
				price = align(price, volume);
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
							text: "Occurrence"
						},
						min: 0,
						maxPadding: 0,
						labels: {
							align: "right",
							style: {
								font: FONT,
								color: "#000"
							},
							x: -5,
						},
						plotLines: [{
							value: 0,
							width: 2,
							color: "silver"
						}],
						height: 200,
						lineWidth: 2,
						lineColor: "silver"
					},
					{
						title: {
							style: {
								font: FONT,
								color: "#000"
							},
							text: "Sentiment"
						},
						maxPadding: 0,
						minPadding: 0,
						alignTicks: false,
						labels: {
							align: "right",
							style: {
								font: FONT,
								color: "#000"
							},
							x: -5,
							formatter: function () {
								return (this.value > 0 ? "+" : "") + numFmt(this.value, 0);
							}
						},
						plotLines:[{
							value: 0,
							width: 2,
							color: "silver"
						}],
						top: 250,
						height: 150,
						offset: 0,
						lineWidth: 2,
						lineColor: "silver"
					}],
					series: [{
						name: "Occurrence", 
						lineWidth: 1,
						data: volume,
						states: {
							hover: {
								lineWidth: 1
							}
						}
					},
					{
						name: "Stock price", 
						lineWidth: 1,
						data: price,
						visible: false,
						color: "#4572A7",
						states: {
							hover: {
								lineWidth: 1
							}
						},
						marker: { 
							symbol: "circle" 
						}
					},
					{
						name: "Sentiment", 
						yAxis: 1,
						lineWidth: 1,
						data: sentiment,
						states: {
							hover: {
								lineWidth: 1
							}
						}
					},
					{
						name: "MA", 
						yAxis: 1,
						data: sentiment,
						visible: false,
						lineWidth: 1,
						color: "#000",
						type: "spline",
						states: {
							hover: {
								enabled: false
							}
						}
					}],
					tooltip: {
						useHTML: true,
						formatter: function() {
							var tooltip = "<span style=\"font-family:'Helvetica Neue',Helvetica,Arial,sans-serif\">" + Highcharts.dateFormat("%a, %b %e, %Y", this.x) + 
								"<br/><span style=\"color:" + this.points[0].series.color + "\">" + this.points[0].series.name + "</span>: <b>" + numFmt(this.points[0].y, 0) + "</b>" +
								"<br/><span style=\"color:" + this.points[1].series.color + "\">Sentiment</span>: <b>" + numFmt(this.points[1].y, 2) + "</b>";
							if (this.points[2]) {
								tooltip += "<br/><span style=\"color:" + this.points[2].series.color + "\">Sentiment MA</span>: <b>" + numFmt(this.points[2].y, 2) + "</b>";
							}
							return tooltip + "</span>";
						}
					}
				});
				chart.span = MAX_DATE - MIN_DATE;
				$(".loading-curtain,.loading-img").hide();
			}).error(error);
		}).error(error);
	}).error(error);
}

// initialize buttons
$(".btn").focus(function() {
	$(this)[0].blur(); // fixes FF focus bug
});

// initialize selection box
$("#entity").val("DAX");

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
$("#lower-chart .btn").click(function() {
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
});

// assign upper-chart button handlers
$("#upper-chart .btn").click(function() {
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
});

// assign selection handler 
$("select").change(function() {
	$(this)[0].blur(); // rmv ugly focus rectangle
	load($("select option:selected").attr("value"));
});

// initialize
load("GDAXI"); // DAX