<!DOCTYPE html> 
<html>
<head>
<meta name="viewport" content="width=1020px" />
<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
<link rel="shortcut icon" href="images/favicon.ico?v=2" />
<script type="text/javascript" src="animator.js"></script>
<script type="text/javascript" src="jquery-1.9.0.min.js"></script>
<script type="text/javascript" src="bezier.js"></script>
<script type="text/javascript">
	var path=[{"x":0,"y":0.10996119016817595},{"x":0.08408796895213455,"y":0.04269081500646831},
	{"x":0.1811125485122898,"y":0.1681759379042691},{"x":0.2664941785252264,"y":0.01811125485122898},
	{"x":0.38163001293661064,"y":0.1034928848641656},{"x":0.40232858990944376,"y":0.06597671410090557},
	{"x":0.5109961190168176,"y":0.09702457956015524},{"x":0.5653298835705046,"y":0.01034928848641656},
	{"x":0.6093143596377749,"y":0.00646830530401035},{"x":0.6558861578266495,"y":0.10737386804657181},
	{"x":0.7192755498059509,"y":0.13454075032341528},{"x":0.77490297542044,"y":0},
	{"x":0.8576972833117724,"y":0.0815006468305304},{"x":0.9573091849935318,"y":0.037516170763260026},
	{"x":1,"y":0.09314359637774904}];
	
	var lines=<?php print(file_get_contents("lines.json"));?>;
	
	var c;
	var ctx;
	
	function load(){
		c=document.getElementById("canvas");
		ctx=c.getContext("2d");
		
		c.width=c.offsetWidth;
		c.height=c.offsetHeight;
		
		var width=0.20;
		var offset=0.5;
		var mergingWidth=0.10;
		
		var outOffset=0.4;
		
		var nbPath=72;
		
		var positionTop=0.3;
		
		var enter=true;
		
		//$(".line.left").css({"margin-left":Math.round(-width*0.5*c.width)+"px"});
		//$(".line.right").css({"margin-left":Math.round(width*0.5*c.width)+"px"});
		
		var bezierPath=getBezier(path);
		
		var bezierPathVector=produceVector(bezierPath);
		
		offset=getPcOffset(bezierPathVector, offset);
		width=getPcWidth(bezierPathVector, width, offset);
		
		var maxLeftDisplacement=c.width*0.05;
		var height=offset*c.width/1365*51;
		
		var middleHeight=bezierPathVector(offset).y;
		
		var centeredPath=translateF(bezierPathVector, new v(0, -middleHeight));
		
		var arrayOfPath=[];
		var arrayOfLeftDisplacement=[];
		var durationArray=[];
		var offsetArray=[];
		
		for (var i=0; i<nbPath; i++){
			var pc=i/(nbPath-1);
			var amplitude=(((1-pc)*(1-pc)*0.7+0.3*Math.random())*1.5-0.5);
			ampitude=amplitude*amplitude*amplitude*amplitude;
			var scaledPath=scaleMF(centeredPath, new v(c.offsetWidth*(1+2*outOffset), 5*amplitude*c.offsetWidth/4));
		
			var translatedPath=translateF(scaledPath, new v(-c.offsetWidth*outOffset, pc*height+c.offsetHeight*positionTop));
		
			var finalPath=clamp(translatedPath, width, offset, mergingWidth);
			
			arrayOfLeftDisplacement.push(pc*maxLeftDisplacement);
			durationArray.push(0.3+pc*0.25+Math.random()*0.25);
			offsetArray.push(pc*0.5+Math.random()*0.5);
			
			var subL=new subLine(finalPath);
			subL.updateOffset(0);
			subL.updateLength(width);
			
			arrayOfPath.push(subL);
		}
		
		var fullDraw=function(pc){
			c.width=c.width;
			ctx.strokeStyle="#8b1e2b";
			ctx.strokeStyle="#d0782f";
			//ctx.lineWidth=(1.5/(nbPath-1))*height;
			//ctx.strokeStyle="#ff8621";
			var stop=offset;
			
			for (var i=0; i<nbPath; i++){
				var subL=arrayOfPath[i];
				var pc_=cutPc(pc,durationArray[i],offsetArray[i]);
				if (enter){
					pc_=Math.sin(pc_*Math.PI/2);
					pc_=pc_*stop;
					var disPc=(1-pc)*(1-pc)*(1-pc);
				}
				else{
					pc_=1-Math.cos(pc_*Math.PI/2);
					pc_=stop+pc_*(1-stop);
					var disPc=pc*pc*pc*pc;
				}
				subL.updateOffset(pc_);
				//subL.updateLength(width);
				var leftMoved=translateF(subL.f,new v(disPc*arrayOfLeftDisplacement[i], 0));
				draw(leftMoved, i);
			}
		}
		
		
		/*var onScroll=function(){
			var pc=$(window).scrollTop()/($(document).height()-$(window).height());
			fullDraw(pc);
		}
		$(window).scroll(onScroll);
		onScroll();*/
		
		
		//draw(finalPath);
		Animator.setTimeout(function(){
			Animator.setAnimation(function(pc){
				fullDraw(pc);
			},6000);
		},500);
		
		Animator.setTimeout(function(){
			$("#epfl, #title").removeClass("hide");
			$("#epfl, #title").addClass("show");
		},6000);
		
		Animator.setTimeout(function(){
			$("#names").removeClass("hide");
			$("#names").addClass("show");
		},7000);
		Animator.setTimeout(function(){
			$("#enter").removeClass("hide");
			$("#enter").addClass("show");
		},8000);
		
		$("#enter").click(function(){
			enter=false;
			$("#epfl, #title, #names, #enter").removeClass("show");
			$("#epfl, #title, #names, #enter").addClass("hide");
			Animator.setTimeout(function(){
				Animator.setAnimation(function(pc){
					fullDraw(pc);
				},5000,function(){
					window.location.href="./enter.php";
				});
			},500);
		})
		
		
	}
	
	
	
	function cutPc(pc, duration, offset){
		var window=(1-duration);
		var start=offset*window;
		var end=start+duration;
		if (pc<start){
			return 0;
		}
		else if (pc>=start && pc<end){
			return (pc-start)/(end-start);
		}
		else{
			return 1;
		}
	}
	
	function resize(){
		c.width=c.offsetWidth;
		c.height=c.offsetHeight;
	}
	function draw(f, i){
		var line=lines[i];
		if (line.length>1){
			var nbPoint=line.length;
			ctx.beginPath();
			
			for (var i=1; i<nbPoint-1; i+=2){
				point=f(line[i]);
				ctx.moveTo(point.x,point.y);
				point=f(line[i+1]);
				ctx.lineTo(point.x,point.y);
			}
			
			ctx.stroke();
		}
	}
	function produceVector(f){
		return function(pc){
			return new v(f(pc));
		}
	}
	function scaleF(f,scale){
		return function(pc){
			return f(pc).scale(scale);
		}
	}
	function scaleMF(f,v2){
		return function(pc){
			return f(pc).scaleM(v2);
		}
	}
	function translateF(f,v2){
		return function(pc){
			return f(pc).add(v2);
		}
	}
	function clamp(f, width, offset, mergingWidth){
		var mergingFunc=function(pc){
			return -Math.cos(pc*Math.PI)/2+0.5;
			return pc;
		}
		var middlePoint=f(offset).y;
		var line=function(pc){
			return new v(pc,middlePoint);
		}
		return function(pc){
			var halfWidth=width/2;
			var mergingStart=offset-halfWidth-mergingWidth;
			var clampStart=offset-halfWidth;
			var clampEnd=offset+halfWidth;
			var mergingEnd=offset+halfWidth+mergingWidth;
			var fV=f(pc);
			var lineV=new v(fV.x, middlePoint);
			if (pc>=mergingStart && pc<=clampStart){
				var start=mergingStart;
				var end=clampStart;
				var pc_=(pc-start)/(end-start);
				pc_=mergingFunc(pc_);
				return fV.merge(lineV, 1-pc_);
			}
			else if (pc>clampStart && pc<=clampEnd){
				var fVStartX=f(clampStart).x;
				var fVEndX=f(clampEnd).x;
				var pc_=(pc-clampStart)/(clampEnd-clampStart);
				return new v(pc_*(fVEndX-fVStartX)+fVStartX, middlePoint);
			}
			else if (pc>clampEnd && pc<=mergingEnd){
				var start=clampEnd;
				var end=mergingEnd;
				var pc_=(pc-start)/(end-start);
				pc_=mergingFunc(pc_);
				return fV.merge(lineV, pc_);
			}
			else{
				return fV;
			}
		}
	}
	function getPcWidth(f, realLength, offset){
		var nbStep=100;
		var actPc=1;
		var actStep=0.5;
		var subF=new subLine(f);
		subF.updateOffset(offset);
		subF.updateLength(actPc);
		
		var actLength=subF.f(1).x-subF.f(0).x;
		for (var i=0; i<nbStep; i++){
			if (actLength>realLength){
				actPc-=actStep;
				subF.updateLength(actPc);
			}
			else{
				actPc+=actStep;
				subF.updateLength(actPc);
			}
			actStep/=2;
			actLength=subF.f(1).x-subF.f(0).x;
		}
		return actPc;
	}
	function getPcOffset(f, offset){
		var nbStep=100;
		var actPc=1;
		var actStep=0.5;
		
		var actOffset=f(actPc).x;
		for (var i=0; i<nbStep; i++){
			if (actOffset>offset){
				actPc-=actStep;
			}
			else{
				actPc+=actStep;
			}
			actStep/=2;
			actOffset=f(actPc).x;
		}
		return actPc;
	}
	function getPcLength(f, realLength, offset){
		var nbStep=100;
		var actPc=1;
		var actStep=0.5;
		var subF=new subLine(f);
		subF.updateOffset(offset);
		subF.updateLength(actPc);
		
		var actLength=lineLength(subF.f);
		for (var i=0; i<nbStep; i++){
			if (actLength<realLength){
				actPc-=actStep;
				subF.updateLength(actPc);
			}
			else{
				actPc+=actStep;
				subF.updateLength(actPc);
			}
			actStep/=2;
			actLength=lineLength(subF.f);
		}
		return actPc;
	}
	function subLine(f_){
		var internalPc=0;
		var pcLength=1;
		var offset=internalPc*(1-pcLength);
		this.updateOffset=function(pc){
			internalPc=pc;
			offset=internalPc*(1-pcLength);
		}
		this.updateLength=function(pcLength_){
			pcLength=pcLength_;
			offset=internalPc*(1-pcLength);
		}
		this.f=function(pc){
			return f_(offset+pc*(pcLength));
		}
	}
	function lineLength(f){
		var nbStep=100;
		var total=0;
		for (var i=0; i<nbStep; i++){
			var pc=i/nbStep;
			var pcNext=(i+1)/nbStep;
			var v1=f(pc);
			var v2=f(pcNext);
			total+=v2.sub(v1).length;
		}
		return total;
	}
	function v(x,y){
		if (y!=undefined){
			this.x=x;
			this.y=y;
		}
		else{
			this.x=x.x;
			this.y=x.y;
		}
		var v_=this;
		this.length=function(){
			return Math.sqrt(v_.x*v_.x+v_.y*v_.y);
		}
		this.sub=function(v2){
			return new v(
				v_.x-v2.x,
				v_.y-v2.y
			);
		}
		this.add=function(v2){
			return new v(
				v_.x+v2.x,
				v_.y+v2.y
			);
		}
		this.scale=function(s){
			return new v(
				v_.x*s,
				v_.y*s
			);
		}
		this.scaleM=function(v2){
			return new v(
				v_.x*v2.x,
				v_.y*v2.y
			);
		}
		this.merge=function(v2,t){
			return v_.scale(t).add(v2.scale(1-t));
		}
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
		font-family:din;
		font-size:12px;
	}
	#canvas{
		position:fixed;
		top:0;
		left:0;
		width:100%;
		height:100%;
	}
	#line{
		position:absolute;
		top:0;
		left:0;
		width:1px;
		height:1000%;
		display:none;
	}
	#container{
		position:fixed;
		top:30%;
		margin-top:50px;
		left:50%;
		margin-left:-20%;
		width:35.75%;
		height:200px;
	}
	#header{
		width:100%;
		margin-bottom:20px;
	}
	#epfl{
		width:37%;
	}
	#title{
		float:right;
		width:60%;
	}
	#names{
		width:100%;
		font-family:din_lightregular;
		font-size:150%;
		color:#e0782f;
		text-align:justify;
	}
	#names:after{
		content:"";
		display:inline-block;
		width:100%;
	}
	.unbreakable{
		display:inline-block;
	}
	#enter{
		font-size:150%;
		text-transform:uppercase;
		color:#8b1e2b;
		cursor:pointer;
		text-align:right;
	}
	#enter:hover{
		opacity:0.5;
	}
	#triangle{
		position:relative;
		top:-2px;
	}
	.show{
		-webkit-transition: all 1.7s ease;
		-moz-transition: all 1.7s ease;
		-o-transition: all 1.7s ease;
		transition: all 1.7s ease;
		
		opacity:1;
		-webkit-filter: blur(0px);
		-moz-filter: blur(0px);
		-o-filter: blur(0px);
		-ms-filter: blur(0px);
		filter: blur(0px);
	}
	
	.hide{
		-webkit-transition: all 1.2s ease;
		-moz-transition: all 1.2s ease;
		-o-transition: all 1.2s ease;
		transition: all 1.2s ease;
	
		opacity:0;
		-webkit-filter: blur(30px);
		-moz-filter: blur(30px);
		-o-filter: blur(30px);
		-ms-filter: blur(30px);
		filter: blur(30px);
	}
</style>
<title>Event Stream Detection</title>
</head>
<body onload="load();">
	<canvas id="canvas"></canvas>
	<div id="line"></div>
	<div id="container">
		<div id=header>
			<img id="epfl" class="hide" src="images/epfl_.png">
			<img id="title" class="hide" src="images/title.png">
		</div>
		<div id="names" class="hide">
			<span class="unbreakable">Laurent Anadon</span>
			<span class="unbreakable">Antoine Bastien</span>
			<span class="unbreakable">Antoine Bodin</span>
			<span class="unbreakable">Matias Cerchierini</span>
			<span class="unbreakable">Nina Desnica</span>
			<span class="unbreakable">Louis Faucon</span>
			<span class="unbreakable">Damien Hilloulin</span>
			<span class="unbreakable">Christian Mouchet</span>
			<span class="unbreakable">Sami Perrin</span>
		</div>
		<div id="enter" class="hide">
			Enter <span id="triangle">&#9656;</span>
		</div>
	</div>
</body>
</html>
