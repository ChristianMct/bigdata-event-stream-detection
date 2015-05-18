<!DOCTYPE html> 
<html>
<head>
<meta name="viewport" content="width=1020px" />
<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
<link rel="shortcut icon" href="images/favicon.ico?v=2" />
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
		padding-right:0;
		
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
<title>Event Stream Detection</title>
</head>
<body onload="load();">
	<div id="menu" class="margin">
		<a class="menuEntry title" href="./"><img id="home" src="images/home.png"></a>
		<a class="menuEntry selected"  href="./enter.php">Event Stream Detection</a>
		<a class="menuEntry" href="./themes.php">Browse themes</a>
		<a class="menuEntry" href="./graph.php">Explore graph</a>
		<span class="menuEntry search"><input type="text" value="Search themes"></span>
	</div>
	<div id="aboutJumb" class="margin">
		<h1>Event Stream Detection</h1>
		<p>As part of the 2015 Big Data course, we are leading a project adressing events detection in news streams.</p>
	</div>
	<div id="about" class="margin">
		<p>
			<h3>Introduction</h3>
			Nowadays, data sets are so big and technology has lowered the barriers to entry so 
			we are able to achieve things that people have never imagined before. Today, we 
			can analyze huge amount of data, collected hundred year before we were born and 
			make the valuable conclusions that can help us in the future. The ability to do that is powerful.
			
			<h3>Goal</h3>
			The main goal of this project is to distinguish articles that are written 
			about the same topic over a set of issues contiguous in time. The biggest 
			challenge in this aim is huge amount of data, as we are considering articles 
			for almost <b>200 years of archive</b>. In order to accomplish this and overcome 
			all issues and challenges the algorithms we use should be scalable.
			
			<h3>Dataset</h3>
			The dataset for this project is provided by the DHLab. It consists of 
			archives from the Swiss newspaper "<b>Le Temps</b>". This historical database 
			comprises 2 newspapers over 200 years:
			<ul>
			<li><i>Journal de Genève</i> (JDG) from 1826 to 1998,</li>
			<li><i>Gazette de Lausanne</i> (GDL, under different names) from 1798 to 1998.</li>
			</ul>
			
			<h3>Design</h3>
			After considering many possible implementations the strategy to use 
			<b>Temporal Text Mining for Discovering Evolutionary Theme Patterns</b> 
			from Text was the most suitable for this task. The main idea is to use 
			Expectation–maximization (EM) algorithm in order to extract themes from 
			a set of documents, then derive an evolution graph and analyze the themes 
			life cycle. One of the TTM’s tasks is discovering and summarizing the 
			evolutionary patterns of themes in a text stream. This new text mining 
			problem could be solved by 
			<ul>
			<li>Discovering latent themes from text</li>
			<li>Constructing an evolution graph of themes</li>
			<li>Analyzing life cycles of themes</li>
			</ul>
			Evaluation of the proposed methods shows that can discover interesting 
			evolutionary theme patterns effectively.
			
			<h3>Browsing themes</h3>
			In the section <i>Browse Theme</i> you will be able to browse the theme 
			we were able to extract from the archive. A theme is represented as a list of 
			word and for each word the probability that it belongs to the theme. From that 
			you will be able to have a general idea on what a theme correspond to. For instance 
			a theme about space travel and lunar exploration would contain the words 
			<i>apollo</i>, <i>lunaire</i>, <i>astronautes</i> and <i>espace</i> for instance.
			For each theme, we analysed for a given period at wich rate the archive talks about it.
			By clicking on it you will be able to discover the 15 most important words it 
			contains and its score per period. For some theme we do not have the score data so 
			will only be able to see the list of words it contains.
			
			<br>
		</p>
	</div>
</body>
</html>
