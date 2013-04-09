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