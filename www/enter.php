<!DOCTYPE html> 
<html>
<head>
<meta name="viewport" content="width=1020px" />
<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
<link rel="shortcut icon" href="images/favicon.ico" />
<script type="text/javascript" src="jquery-1.9.0.min.js"></script>
<script type="text/javascript">
	function load(){
		var onScroll=function(){
			if ($(window).scrollTop()>270){
				$("#menu").addClass("more");
			}
			else{
				$("#menu").removeClass("more");
			}
		}
		$(window).scroll(onScroll);
		onScroll();
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
		margin-top:60px;
		background-color:#fcfcfc;
	}
	#menu{
		position:fixed;
		top:0;
		padding-top:0;
		padding-bottom:0;
		text-transform:uppercase;
		font-family:din;
		font-size:150%;
		color:#8b1e2b;
		padding-left:80px;
		background-color:white;
		
	}
	#menu.more{
		box-shadow: 0px 0px 5px #aaaaaa;
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
	}
	
	.menuEntry.title{
		/*font-size:110%;*/
	}
	
	.menuEntry.selected{
		background-color:#f1f1f1;
	}
	
	input{
		display:none;
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
	#aboutJumb{
		background-color:#8b1e2b;
		background-color:#d0782f;
		color:white;
		padding-top:40px;
		padding-bottom:40px;
		font-size:150%;
	}
	.margin{
		width:100%;
		margin:0;
		padding-top:10px;
		padding-left:100px;
		padding-right:30%;
		padding-bottom:10px;
		box-sizing: border-box;
	}
	#about{
		text-align:justify;
	}
	#home{
		height:17px;
	}
</style>
<title></title>
</head>
<body onload="load();">
	<div id="menu" class="margin">
		<a class="menuEntry title" href="./"><img id="home" src="images/home.png"></a>
		<a class="menuEntry selected"  href="./enter.php">Event Stream Detection</a>
		<a class="menuEntry" href="./themes.php">Browse themes</a>
		<span class="menuEntry search"><input type="text" value="Search themes"></span>
	</div>
	<div id="aboutJumb" class="margin">
		<h1>Event Stream Detection</h1>
		<p>As part of the 2015 Big Data course, we are leading a project adressing events detection in news streams.</p>
	</div>
	<div id="about" class="margin">
		<p>
  As part of the 2015 Big Data course, we are leading a project adressing event/topic detection in news streams. We are nine people working on this project. For details about our team composition, please refer <a href="http://wiki.epfl.ch/bd15-esd/team">here</a>.
</p>
<p>
  For everything related to the development environment, please refer <a href="http://wiki.epfl.ch/bd15-esd/dev-env">here</a>.
</p>
<h3>Dataset</h3>
<p>
  The dataset for this project is provided by the DHLab. It consists of archives from the Swiss newspaper &quot;Le Temps&quot;. This historical database comprises 2 newspapers over 200 years :
</p>
<ul style="margin-left: 40px;">
  <li>&ldquo;Journal de Gen&egrave;ve&rdquo; (JDG) from 1826 to 1998,</li>
  <li>&ldquo;Gazette de Lausanne&rdquo; (GDL, under different names) from 1798 to 1998.</li>
</ul>
<h3>Goal</h3>
<p>
  We aim at detecting articles that talk about the same topic over a set of issues contiguous in time, and across the first two newspapers. To do this, we are looking into clustering, hierarchical clustering and correlations detection techniques. One of the main challenges here is the huge amount of data : we are considering articles for almost 200 years, which is why we need the algorithms we implement to be scalable.
</p>
<h3>Project calendar and milestones</h3>
<p>
  In a first phase, we are looking into various research papers that are tackling similar problems, a discussion of these papers content can be found <a href="http://wiki.epfl.ch/bd15-esd/papers">here</a>. At the end of this first time, the goal is to be able to have a formal definition of what a topic is and to have chosen the two or three methods/algorithms that appear to be the best suited for our project, both in terms of efficiency and scalability. The deadline for the end of this first phase is :
</p>
<p style="margin-left: 40px;">
  <strong>milestone 1 : March 18</strong>
</p>
<p>
  Then we will begin to implement the algorithms. We want to first have a running implementation even if it is running sequentially on a small subset of the whole dataset. This phase is to be finished by :
</p>
<p style="margin-left: 40px;">
  <strong>milestone 2 : April 15</strong>
</p>
<p>
  At milestone 2, we had several pieces of code working independently from each other. We then enter an integrating and optimzing part of our project. This part is to be done by new milestone 2b in order to be able to assess what we should focus on in the last two weeks.
</p>
<p style="margin-left: 40px;">
  <strong>milestone 2b : April 30</strong>
</p>
<p>
  Last, we will work on the scalability of our implementations, run various experiments, extract results, try to interpret these results. The end of this last phase is also the end of the project :
</p>
<p style="margin-left: 40px;">
  <strong>milestone 3 : May 12</strong>
</p>
	</div>
</body>
</html>
