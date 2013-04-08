var DAY_SPAN
	= 86400000;
var FONT
	= "12px 'Helvetica Neue',Helvetica,Arial,sans-serif";

function error() {
	$(".loading-img").hide(); $(".loading-oops").show();
}

function dateOnly(ms) {
	var d = new Date(ms);
	return Date.UTC(d.getFullYear(), d.getMonth(), d.getDate());
}

function getOption(sel) {
	return $(sel + " .active").attr("id");
}

function getSelection(sel) {
	return $(sel + " option:selected").attr("value");
}

function showLoading() {
	$(".loading-curtain,.loading-img").show();
}

function hideLoading() {
	$(".loading-curtain,.loading-img").hide();
}

function MA(data, days) {
	var ma = [];
	var queue = [];
	var sum = 0;
	for (var i in data) {
		if (queue.length < days) {
			queue.push(data[i]); sum += data[i]; 
			ma.push(sum / queue.length);
		} else {
			queue.push(data[i]); sum += data[i];
			sum -= queue.shift();
			ma.push(sum / days);
		}
	}
	return ma;
}

function aaSum(a, b) {
	r = []; for (var i in a) { r.push(a[i] + b[i]); } return r;
}

function asSum(a, b) { 
	r = []; for (var i in a) { r.push(a[i] + b); } return r;	
}

function aaSub(a, b) {
	r = []; for (var i in a) { r.push(a[i] - b[i]); } return r;
}

function asMult(a, b) {
	r = []; for (var i in a) { r.push(a[i] * b); } return r;
}

function aaMult(a, b) {
	r = []; for (var i in a) { r.push(a[i] * b[i]); } return r;
}

function aaDiv(a, b) {
	r = []; for (var i in a) { r.push(a[i] / b[i]); } return r;
}

function toggleSeries(chart, seriesHide, seriesShow, title) {
	for (var i in seriesHide) {
		chart.series[seriesHide[i]].setVisible(false, false);
		chart.series[seriesHide[i]].yAxis.setTitle({ text: null }, false);
	}
	for (var i in seriesShow) {
		chart.series[seriesShow[i]].setVisible(true, false);
		chart.series[seriesShow[i]].yAxis.setTitle({ text: title }, false);
	}
	chart.redraw();
}