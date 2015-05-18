<!DOCTYPE html> 
<html>
<head>
<meta name="viewport" content="width=1020px" />
<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
<link rel="shortcut icon" href="images/favicon.ico?v=2" />
<script type="text/javascript" src="jquery-2.1.4.min.js"></script>
<script type="text/javascript" src="masonry.pkgd.min.js"></script>
<script type="text/javascript" src="Chart.min.js"></script>
<script type="text/javascript" src="datepicker/js/jquery-ui-1.10.2.custom.min.js"></script>
<link href="datepicker/css/smoothness/jquery-ui-1.10.2.custom.css" rel="stylesheet">
<script type="text/javascript">
	var themes=[];
	var defaultNbWordPerTheme=5;
	var maxNbWordPerTheme=15;
	var nbDigit=100; //(in log 10)
	var container;
	var loadingAjaxInAction=false;
	var masonryStarted=false;
	var noMoreTheme=false;
	function load(){
		$.datepicker.setDefaults({
			changeYear: true,
			yearRange: "1800:1998"
		});
		loadTheme();
		var onScroll=function(){
			if (!loadingAjaxInAction && !noMoreTheme){
				var pc=$(window).scrollTop()/($(document).height()-$(window).height());
				if ($(window).scrollTop()+20 > $(document).height()-$(window).height()){
					$("#loadingAjax").addClass("shown");
					loadingAjaxInAction=true;
					loadTheme();
				}
			}
		}
		$(window).scroll(onScroll);
	}
	function loadTheme(){
		$.ajax("loadFrom.php?from="+themes.length)
		.done(function(data){
			//console.log(data);
			var newThemes=JSON.parse(data);
			if (newThemes.length>0){
				displayNewThemes(newThemes);
			}
			else{
				noMoreTheme=true;
			}
			$("#loading").addClass("hidden");
			$("#loadingAjax").removeClass("shown");
			loadingAjaxInAction=false;
		});
	}
	function displayNewThemes(newThemes){
		var elems=[];
		var domElems=[];
		for (var i=0; i<newThemes.length; i++){
			var themeElem=getThemeElem(newThemes[i]);
			elems.push(themeElem);
			domElems.push(themeElem.get(0));
			themes.push(newThemes[i]);
		}
		if (!masonryStarted){
			$("#content").append(elems);
			$("#content").masonry({
				columnWidth: 10
			});
			masonryStarted=true;
		}
		else{
			//console.log("append new!");
			$("#content").append(elems);
			$("#content").masonry( 'appended', domElems );
		}
		//container.append(elems).masonry('appended', elems);
		$(".theme").not(".full").off("click");
		$(".theme").not(".full").click(function(){
			displayFullTheme($(this));
			//displayNoDataTheme($(this));
		})
	}
	function displayFullTheme(themeElem){
		if (themeElem.attr("data-no-data")=="true"){
			displayNoDataTheme(themeElem);
			return;
		}
		themeElem.addClass("full");
		$("#content").masonry("layout");
		
		themeElem.off("click");
		
		var themeIndex=themeElem.attr("data-theme-index")*1;
		//console.log(themeIndex);
		var theme=themes[themeIndex];
		
		var htmlString="";
		
		var nbWord=Math.min(theme["wl"].length,maxNbWordPerTheme);
		//alert(theme["wl"].length);
		//console.log(nbWord);
		
		for (var i=defaultNbWordPerTheme; i<nbWord; i++){
			//console.log(theme["wl"][i]);
			//console.log(i);
			htmlString+=wordStringFull(theme["wl"][i],i);
		}	
		
		$(".themeTable", themeElem).append($(htmlString));
		
		var chartContainer=$("<canvas class='chartContainer fullElem loading'></canvas>");
		themeElem.append(chartContainer);
		
		var closeElem=$("<div class='close fullElem'>close</div>");
		closeElem.click(function(){
			var themeElem=$(this).parent();
			themeElem.removeClass("full");
			$(".fullElem", themeElem).remove();
			$("#content").masonry();
			setTimeout(function(){
				themeElem.click(function(){
					displayFullTheme($(this));
				});
			},10);
		})
		themeElem.append(closeElem);
		
		var from=theme["start"];
		var to=theme["end"];
		
		var fromYear=from.split("/")[2];
		var toYear=to.split("/")[2];
		
		var startElem=$("<input class='date start fullElem' value='"+from+"'>");
		startElem.datepicker({
			onSelect:function(date){
				from=date;
				loadData(from, to, themeIndex, themeElem);
			},
			yearRange: fromYear+":"+toYear
		});
		themeElem.append(startElem);
		
		var endElem=$("<input class='date end fullElem'  value='"+to+"'>");
		endElem.datepicker({
			onSelect:function(date){
				to=date;
				loadData(from, to, themeIndex, themeElem);
			},
			yearRange: fromYear+":"+toYear
		});
		themeElem.append(endElem);
		
		var periodElem=$("<span class='period fullElem'></span>");
		themeElem.append(periodElem);
		
		//loadChart(chartContainer, themeIndex);
		loadData(from, to, themeIndex, themeElem);
		
	}
	function displayNoDataTheme(themeElem){
		themeElem.addClass("full noData");
		$("#content").masonry("layout");
		themeElem.off("click");
		
		var themeIndex=themeElem.attr("data-theme-index")*1;
		var theme=themes[themeIndex];
		
		var nbWord=Math.min(theme["wl"].length,maxNbWordPerTheme);
		var htmlString="";
		for (var i=defaultNbWordPerTheme; i<nbWord; i++){
			htmlString+=wordStringFull(theme["wl"][i],i);
		}	
		$(".themeTable", themeElem).append($(htmlString));
		
		var closeElem=$("<div class='close fullElem'>close</div>");
		closeElem.click(function(){
			var themeElem=$(this).parent();
			themeElem.removeClass("full");
			$(".fullElem", themeElem).remove();
			$("#content").masonry();
			setTimeout(function(){
				themeElem.click(function(){
					displayFullTheme($(this));
				});
			},10);
		});
		themeElem.append(closeElem);
	}
	function loadData(from, to, themeIndex, themeElem){
		$.ajax("loadThemeData.php?from="+from+"&to="+to+"&themeIndex="+themeIndex)
		.done(function(data){
			var data=JSON.parse(data);
			
			var chartElem=$(".chartContainer", themeElem);
			chartElem.remove();
			
			$(".period", themeElem).html("Interval between values: <b>"+data["period"]+"</b>");
			
			var chartContainer=$("<canvas class='chartContainer fullElem loading'></canvas>");
			themeElem.append(chartContainer);
			loadChart(chartContainer, themeIndex, data);
		});
	}
	function loadChart(chartElem, themeIndex, data){
		var ctx = chartElem.get(0).getContext("2d");
		var options={
			bezierCurve:true,
			showLabels:false
		};
		if (data["period"]=="one day"){
			options.bezierCurve=false;
		}
		var content=[];
		var contentData=[];
		for (var i=0; i<30; i++){
			content[i]=i;
			contentData[i]=Math.random();
		}
		var data = {
			labels: data["contentDate"],
			datasets: [
				{
					label: "My first dataset",
					fillColor: "rgba(151,187,205,0.2)",
					strokeColor: "rgba(151,187,205,1)",
					pointColor: "rgba(151,187,205,1)",
					pointStrokeColor: "#fff",
					pointHighlightFill: "#fff",
					pointHighlightStroke: "rgba(151,187,205,1)",
					data: data["contentData"]
				}
			]
		};
		var myLineChart = new Chart(ctx).Line(data, options);
		chartElem.removeClass("loading");
	}
	function getThemeElem(theme){
		if (theme["noData"]=="true"){
			var noData="true";
		}
		else{
			var noData="false";
		}
		var htmlString=
			"<div class='theme' data-theme-index='"+theme["i"]+"' data-no-data='"+noData+"'>" +
			"<table class='themeTable'>" +
				"<tr>" +
					"<th>#</th>" +
					"<th>Word</th>" +
					"<th>&#8240;</th>" +
				"</tr>";

		for (var i=0; i<defaultNbWordPerTheme; i++){
			htmlString+=wordString(theme["wl"][i],i);
		}	
			
		htmlString += 
			"</table>" +
			"<br>" +
			"<span class='more'>more</span>" +
			"</div>";
			
		return $(htmlString);
	}
	function wordString(word, index){
		return "<tr>" +
				"<td>"+(index+1)+"</td>" +
				"<td>"+word["w"]+"</td>" +
				"<td>"+perMille(word["p"])+"</td>" +
			"</tr>";
	}
	function wordStringFull(word, index){
		//console.log(word);
		return "<tr class='fullElem'>" +
				"<td>"+(index+1)+"</td>" +
				"<td>"+word["w"]+"</td>" +
				"<td>"+perMille(word["p"])+"</td>" +
			"</tr>";
	}
	function perMille(value){
		return Math.round(value*1000*nbDigit)/nbDigit;
	}
</script>
<style type="text/css">
	@font-face {
		font-family: 'din';
		src: url('font/din/din.eot');
		src: url('font/din/din.eot?#iefix') format('embedded-opentype'),
			 url('font/din/din.woff') format('woff'),
			 url('font/din/din.ttf') format('truetype'),
			 url('font/din/din.svg#din') format('svg');
		font-weight: normal;
		font-style: normal;
	}
	@font-face {
		font-family: 'din_lightregular';
		src: url('font/din_lightregular/ufonts.com_din-light-webfont.eot');
		src: url('font/din_lightregular/ufonts.com_din-light-webfont.eot?#iefix') format('embedded-opentype'),
			 url('font/din_lightregular/ufonts.com_din-light-webfont.woff2') format('woff2'),
			 url('font/din_lightregular/ufonts.com_din-light-webfont.woff') format('woff'),
			 url('font/din_lightregular/ufonts.com_din-light-webfont.ttf') format('truetype'),
			 url('font/din_lightregular/ufonts.com_din-light-webfont.svg#din_lightregular') format('svg');
		font-weight: normal;
		font-style: normal;
	}
	body{
		font-family:din_lightregular;
		font-size:13px;
		margin:0;
		padding:0;
		background-color:#f0f0f0;
		padding-top:70px;
	}
	#menu{
		padding-top:0;
		padding-bottom:0;
		text-transform:uppercase;
		font-family:din;
		font-size:150%;
		color:#8b1e2b;
		padding-left:80px;
		background-color:white;
		position:fixed;
		top:0;
		box-shadow: 0px 0px 5px #aaaaaa;
		z-index:2;
	}
	h2, h3{
		text-transform:uppercase;
		font-family:din;
		color:#8b1e2b;
		font-size:150%;
	}
	a{
		color:#8b1e2b;
		text-decoration:none;
	}
	a:hover{
		opacity:0.7;
	}
	.menuEntry{
		padding-top:20px;
		padding-bottom:20px;
		padding-right:20px;
		padding-left:20px;
		display:inline-block;
		height:25px;
		font-weight:bold;
	}
	
	.menuEntry.search{
		position:absolute;
		right:100px;
		top:0px;
		display:none;
	}
	
	.menuEntry.title{
		/*font-size:110%;*/
	}
	
	.menuEntry.selected{
		background-color:#f1f1f1;
	}
	
	input{
		border:1px solid #eeeeee;
		border-radius: 5px;
		background-image:url(images/search.png);
		background-size:auto 75%;
		background-position:98% 50%;
		background-repeat:no-repeat;
		padding-right:10px;
		padding-left:10px;
		padding-top:5px;
		padding-bottom:3px;
		color:#aaaaaa;
		font-family:din;
		width:200px;
		
		-webkit-transition: all .5s ease;
		-moz-transition: all .5s ease;
		-o-transition: all .5s ease;
		transition: all .5s ease;
	}
	input:focus{
		color:black;
		box-shadow: 0px 0px 5px #aaaaaa;
		width:350px;
	}
	.margin{
		width:100%;
		margin:0;
		padding-top:10px;
		padding-left:100px;
		padding-right:100px;
		padding-bottom:10px;
		box-sizing: border-box;
	}
	#content{
		postion:absolute;
		top:0;
		left:0;
		text-align:justify;
		overflow: auto;
	}
	#loading{
		position:absolute;
		top:50%;
		left:50%;
		width:32px;
		height:32px;
		margin-top:-16px;
		margin-left:-16px;
	}
	#loading.hidden{
		display:none;
	}
	.theme{
		background-color:white;
		padding:10px;
		border-radius:10px;
		width:200px;
		height:150px;
		margin-right:10px;
		margin-bottom:10px;
		cursor:pointer;
	}
	.more{
		font-size:120%;
	}
	.theme.full{
		width:890px;
		height:330px;
		cursor:initial;
		position:relative;
	}
	.theme.full.noData{
		width:200px;
		height:330px;
		cursor:initial;
		position:relative;
	}
	.theme.full .chartContainer{
		position:absolute;
		top:10px;
		right:10px;
		width:660px;
		height:300px;
		background-color:#f8f8f8;
	}
	.theme.full .chartContainer.loading{
		background-image:url(images/loading.gif);
		background-position:center center;
		background-repeat:no-repeat;
	}
	.theme.full .close{
		position:absolute;
		bottom:10px;
		left:10px;
		cursor:pointer;
		font-size:120%;
	}
	.theme.full .close:hover{
		font-weight:bold;
	}
	.theme.full .more{
		display:none;
	}
	.theme.full input{
		position:absolute;
		bottom:7px;
		width:150px;
		background-image:url(images/inputDate.png);
	}
	.theme.full input:focus{
		width:150px;
	}
	.theme.full input.start{
		left:240px;
	}
	.theme.full input.end{
		left:415px;
	}
	.theme.full .period{
		position:absolute;
		bottom:7px;
		width:150px;
		left:590px;
		color:#555555;
	}
	.themeTable{
		width:200px;
		border-collapse: collapse; 
	}
	td{
		border-bottom:1px solid #eeeeee;
		padding-right:0;
		padding-left:0;
		margin-right:0;
		margin-left:0;
	}
	.theme:hover .more{
		font-weight:bold;
	}
	#loadingAjax{
		text-align:center;
	}
	#loadingAjax img{
		display:none;
	}
	#loadingAjax.shown img{
		display:inline;
	}
	#home{
		height:17px;
	}
</style>
<title>Event Stream Detection</title>
</head>
<body onload="load();">
	<div id="menu" class="margin">
		<a class="menuEntry title" href="./"><img id="home" src="images/home.png"></a>
		<a class="menuEntry"  href="./enter.php">Event Stream Detection</a>
		<a class="menuEntry selected" href="./themes.php">Browse themes</a>
		<a class="menuEntry" href="./graph.php">Explore graph</a>
		<span class="menuEntry search"><input type="text" value="Search themes"></span>
	</div>
	<div id="content" class="margin">
		
	</div>
	<div id="loadingAjax" class="margin">
		<img src="images/loading.gif">
	</div>
	<img src="images/loading.gif" id="loading">
</body>
</html>
