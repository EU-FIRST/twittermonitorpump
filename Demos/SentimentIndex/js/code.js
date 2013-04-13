var chart;

var MIN_DATE 
	= Date.UTC(2012, 0, 1); 
var MAX_DATE 
	= Date.UTC(2012, 11, 31); 

var IDX_PRICE
	= 1;
var IDX_VOL
	= 2;
var IDX_VOLATILITY
	= 3;
var IDX_COUNT_POS
	= 4;
var IDX_COUNT_NEG
	= 5;
var IDX_COUNT_DIFF
	= 6;
var IDX_COUNT_DIFF_MA7
	= 7;
var IDX_COUNT_DIFF_MA14
	= 8;
var IDX_POLARITY
	= 9;
var IDX_POLARITY_MA7
	= 10;
var IDX_POLARITY_MA14
	= 11;
var IDX_COUNT
	= 12;

function showChartUpper(option) {
	var group = [ IDX_PRICE, IDX_VOL, IDX_VOLATILITY ];
	if (option == "uchart-price") {
		toggleSeries(chart, group, [ IDX_PRICE ], "Stock price");
	} else if (option == "uchart-volatility") {
		toggleSeries(chart, group, [ IDX_VOLATILITY ], "Daily volatility");
	} else {
		toggleSeries(chart, group, [ IDX_VOL ], "Trading volume");
	}
}

function showChartLower(option, ma) {
	var group = [];
	for (var i = IDX_COUNT_POS; i <= IDX_COUNT; i++) { group.push(i); }
	if (option == "lchart-diff") {
		var showSeries = [ IDX_COUNT_POS, IDX_COUNT_NEG ];
		if (ma == "lchart-ma-none") { showSeries.push(IDX_COUNT_DIFF); }
		else if (ma == "lchart-ma-7-day") { showSeries.push(IDX_COUNT_DIFF_MA7); }
		else { showSeries.push(IDX_COUNT_DIFF_MA14); }
		toggleSeries(chart, group, showSeries, "Absolute difference");
	} else if (option == "lchart-polar") {
		var showSeries = [ IDX_POLARITY ];
		if (ma == "lchart-ma-7-day") { showSeries.push(IDX_POLARITY_MA7); }
		else if (ma == "lchart-ma-14-day") { showSeries.push(IDX_POLARITY_MA14); }
		toggleSeries(chart, group, showSeries, "Sentiment polarity");
	} else {
		toggleSeries(chart, group, [ IDX_COUNT ], "Tweet count");
	}
}

function load(name) {
	showLoading();
	$("#datepicker-from").data("datetimepicker").setDate(MIN_DATE);
	$("#datepicker-to").data("datetimepicker").setDate(MAX_DATE);
	$("#zoom-all,#uchart-price,#lchart-diff,#lchart-ma-none").button("toggle");
	$.getJSON("data/" + name + ".txt", function(data) {
		var pointStart = MIN_DATE;
		var pointInterval = 3600 * 1000 * 24;
		var vol = aaSum(aaSum(data.obj, data.pos), data.neg);
		var diff = aaSub(data.pos, data.neg);
		var pol = aaDiv(diff, asSum(aaSum(data.pos, data.neg), 1));
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
						var min = event.min;
						var max = event.max;
						if (!min) { min = MIN_DATE; }
						if (!max) { max = MAX_DATE; }
						$("#datepicker-from").data("datetimepicker").setDate(dateOnly(min));
						$("#datepicker-to").data("datetimepicker").setDate(dateOnly(max));
						if (Math.abs(max - min - chart.span) >= 2 * DAY_SPAN) {
							chart.span = max - min;
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
			yAxis: [{ // stock price axis
				title: {
					style: {
						font: FONT,
						color: "#000"
					},
					text: "Stock price"
				},
				maxPadding: 0,
				minPadding: 0,
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
			{ // trading volume axis
				title: {
					style: {
						font: FONT,
						color: "#000"
					}
				},
				maxPadding: 0,
				min: 0,
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
				offset: 0,
				lineWidth: 0,
				lineColor: "silver"
			},
			{ // sentiment difference axis
				title: {
					style: {
						font: FONT,
						color: "#000"
					},
					text: "Absolute difference"
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
					x: -5
				},
				plotLines: [{
					value: 0,
					width: 2,
					color: "silver"
				}],
				top: 230,
				height: 200,
				offset: 0,
				lineWidth: 2,
				lineColor: "silver"
			},
			{ // sentiment polarity axis
				title: {
					style: {
						font: FONT,
						color: "#000"
					}
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
					x: -5
				},
				plotLines: [{
					value: 0,
					width: 2,
					color: "silver"
				}],
				top: 230,
				height: 200,
				offset: 0,
				lineWidth: 0,
				lineColor: "silver"
			},
			{ // tweet count axis
				title: {
					style: {
						font: FONT,
						color: "#000"
					}
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
					x: -5
				},
				plotLines: [{
					value: 0,
					width: 2,
					color: "silver"
				}],
				top: 230,
				height: 200,
				offset: 0,
				lineWidth: 0,
				lineColor: "silver"
			},
			{ // volatility axis
				title: {
					style: {
						font: FONT,
						color: "#000"
					}
				},
				maxPadding: 0,
				minPadding: 0,
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
				offset: 0,
				lineWidth: 0,
				lineColor: "silver"
			}],
			series: [{ // this series shows in navigator
				data: vol,
				visible: false,
				pointStart: pointStart,
				pointInterval: pointInterval
			},
			{ // 1
				name: "Stock price",
				decimals: 2,
				yAxis: 0,
				lineWidth: 1,
				data: data.close,
				visible: true,
				color: "#4572A7",
				states: { hover: { lineWidth: 1 } },
				marker: { symbol: "circle" },
				pointStart: pointStart,
				pointInterval: pointInterval
			},
			{ // 2
				name: "Trading volume",
				decimals: 0,
				yAxis: 1,
				min: 0,
				lineWidth: 1,
				data: data.vol,
				visible: false,
				color: "#4572A7",
				states: { hover: { lineWidth: 1 } },
				marker: { symbol: "circle" },
				pointStart: pointStart,
				pointInterval: pointInterval
			},
			{ // 3
				name: "Daily volatility", 
				decimals: 3,
				yAxis: 5,
				lineWidth: 1,
				color: '#4572A7',
				data: data.volatility,
				visible: false,
				states: { hover: { lineWidth: 1 } },
				marker: { symbol: "circle" },
				pointStart: pointStart,
				pointInterval: pointInterval
			},
			{ // 4
				name: "Positive tweet count", 
				decimals: 0,
				yAxis: 2,
				lineWidth: 1,
				type: "area",
				data: data.pos,
				visible: true,
				color: '#89A54E',
				states: { hover: { lineWidth: 1 } },
				marker: { symbol: "circle" },
				pointStart: pointStart,
				pointInterval: pointInterval
			},
			{ // 5
				name: "Negative tweet count", 
				decimals: 0,
				yAxis: 2,
				lineWidth: 1,
				type: "area",
				data: asMult(data.neg, -1),
				visible: true,
				color: '#AA4643',
				states: { hover: { lineWidth: 1 } },
				marker: { symbol: "circle" },
				pointStart: pointStart,
				pointInterval: pointInterval
			},
			{ // 6
				name: "Absolute difference", 
				decimals: 0,
				yAxis: 2,
				lineWidth: 1,
				color: '#000',
				data: diff,
				visible: true,
				states: { hover: { lineWidth: 1 } },
				marker: { symbol: "circle" },
				pointStart: pointStart,
				pointInterval: pointInterval
			},
			{ // 7
				name: "Absolute difference MA", 
				decimals: 2,
				yAxis: 2,
				lineWidth: 1,
				color: '#000',
				data: asMult(MA(diff, 7), 3),
				visible: false,
				states: { hover: { lineWidth: 1 } },
				marker: { symbol: "circle" },
				pointStart: pointStart,
				pointInterval: pointInterval
			},
			{ // 8
				name: "Absolute difference MA", 
				decimals: 2,
				yAxis: 2,
				lineWidth: 1,
				color: '#000',
				data: asMult(MA(diff, 14), 3),
				visible: false,
				states: { hover: { lineWidth: 1 } },
				marker: { symbol: "circle" },
				pointStart: pointStart,
				pointInterval: pointInterval
			},
			{ // 9
				name: "Sentiment polarity", 
				decimals: 2,
				yAxis: 3,
				lineWidth: 1,
				color: '#AA4643',
				data: pol, 
				visible: false,
				states: { hover: { lineWidth: 1 } },
				marker: { symbol: "circle" },
				pointStart: pointStart,
				pointInterval: pointInterval
			},
			{ // 10
				name: "Sentiment polarity MA", 
				decimals: 2,
				yAxis: 3,
				lineWidth: 1,
				color: '#000',
				data: MA(pol, 7),
				visible: false,
				states: { hover: { lineWidth: 1 } },
				marker: { symbol: "circle" },
				pointStart: pointStart,
				pointInterval: pointInterval
			},
			{ // 11
				name: "Sentiment polarity MA", 
				decimals: 2,
				yAxis: 3,
				lineWidth: 1,
				color: '#000',
				data: MA(pol, 14),
				visible: false,
				states: { hover: { lineWidth: 1 } },
				marker: { symbol: "circle" },
				pointStart: pointStart,
				pointInterval: pointInterval
			},
			{ // 12
				name: "Tweet count", 
				decimals: 0,
				yAxis: 4,
				lineWidth: 1,
				color: '#AA4643',
				data: vol,
				visible: false,
				states: { hover: { lineWidth: 1 } },
				marker: { symbol: "circle" },
				pointStart: pointStart,
				pointInterval: pointInterval
			}],
			tooltip: {
				useHTML: true,
				formatter: function() {
					var tooltip = "<span style=\"font-family:'Helvetica Neue',Helvetica,Arial,sans-serif\"><b>" + Highcharts.dateFormat("%a, %b %e, %Y", this.x) + "</b>";
					for (var i in this.points) {
						var name = this.points[i].series.name;
						tooltip += "<br/><span style=\"color:" + this.points[i].series.color + "\">" + name + ":</span> <b>" + Highcharts.numberFormat(this.points[i].y * (name == "Negative tweet count" ? -1 : 1), this.points[i].series.options.decimals) + "</b>";
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
$("#entity").val("GOOG");

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
	if (action == "zoom-all") { chart.xAxis[0].setExtremes(MIN_DATE, MAX_DATE); return; }
	else if (action == "zoom-1m") { span = 30 * DAY_SPAN; }
	else if (action == "zoom-3m") { span = 90 * DAY_SPAN; }
	else if (action == "zoom-6m") { span = 180 * DAY_SPAN; }
	var dateStart = dateEnd - span;
	if (dateStart < MIN_DATE) { dateEnd += (MIN_DATE - dateStart); dateStart += (MIN_DATE - dateStart); }
	chart.xAxis[0].setExtremes(dateStart, dateEnd);
});

// assign upper-chart button handlers
$("#uchart .btn").click(function() {
	showChartUpper($(this).attr("id"));
});

// assign lower-chart button handlers
$("#lchart .btn").click(function() {
	var action = $(this).attr("id");
	if (action == "lchart-count") { 
		$("#lchart-ma button").addClass("disabled"); 
	} else {
		$("#lchart-ma button").removeClass("disabled"); 
	}
	showChartLower(action, getOption("#lchart-ma"));
});

// assign MA button handlers
$("#lchart-ma .btn").click(function() {
	showChartLower(getOption("#lchart"), $(this).attr("id"));
});

// assign selection handler 
$("select").change(function() {
	$(this)[0].blur(); // rmv ugly focus rectangle
	load(getSelection("#entity"));
});

// initialize
load("GOOG"); 