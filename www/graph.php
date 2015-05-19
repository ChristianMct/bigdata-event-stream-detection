<!DOCTYPE html> 
<html>
<head>
<meta name="viewport" content="width=1020px" />
<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
<link rel="shortcut icon" href="images/favicon.ico" />
<script type="text/javascript" src="jquery-1.9.0.min.js"></script>
<script type="text/javascript" src="jquery.mousewheel.min.js"></script>
<script type="text/javascript" src="svg/svgPathParser.js"></script>
<script type="text/javascript" src="svg/kdTree-min.js"></script>
<script type="text/javascript" src="animator.js"></script>
<script type="text/javascript" src="bezier.js"></script>
<script type="text/javascript">
	window.requestAnimationFrame=window.requestAnimationFrame || window.mozRequestAnimationFrame || window.webkitRequestAnimationFrame || window.msRequestAnimationFrame || function(f){setTimeout(f,1);};
	var svg='<?php print(str_replace("\n","",file_get_contents("svg/graph1992.svg"))); ?>';
	var nodeArray=[];
	var edgeArray=[];
	var nodeToAnimate=[];
	var maxX=Number.MIN_VALUE; 
	var maxY=Number.MIN_VALUE; 
	var minX=Number.MAX_VALUE; 
	var minY=Number.MAX_VALUE; 
	var maxWidth=Number.MIN_VALUE;
	var minWidth=Number.MAX_VALUE;
	var maxLength=Number.MIN_VALUE;
	var minLength=Number.MAX_VALUE;
	var xml;
	var c;
	var ctx;
	var zoom=1;
	var preScale=1;
	var preTranslate;
	var translate;
	var drawingWidth;
	var drawingHeight;
	var drawingRatio;
	var windowWidth;
	var windowHeight;
	var windowRatio;
	var mouseDown=false;
	var previousDraggingPoint;
	var globalAlpha=1;
	var lineWidthCoeff=10;
	var changes=true;
	var nbPoint=30;
	var nbPointSmall=3;
	var prePreScale;
	var mouseOverOneNode=false;
	function load(){
		var xmlDoc = $.parseXML(svg);
		xml = $(xmlDoc);
		prePreScale=new V(1,1.3);
		extractNodes();
		extractEdges();
		matchBeginingEnd();
		
		c=document.getElementById("canvas");
		ctx=c.getContext("2d");
		translate=new V(0,0);
		resize();
		setUpInput();
		$(window).resize(function(){
			resize();
			changes=true;
		})
		draw();
		
	}
	function animate(node){
		if (node==undefined && nodeToAnimate.length==0){
			return;
		}
		
		if (node==undefined){
			nodeToAnimate.sort(function(a,b){
				return nodeArray[a].position.y - nodeArray[b].position.y;
			});
			var firstNode=nodeToAnimate.shift();
			node=nodeArray[firstNode];
		}
		
		node.nodeElem.addClass("show");
		if (node.leavingEdge.length!=0){
			Animator.setAnimation(function(pc){
				pc=-Math.cos(pc*Math.PI)/2+0.5;
				for (var i=0; i<node.leavingEdge.length; i++){
					var edge=edgeArray[node.leavingEdge[i]];
					edge.pc=pc;
					changes=true;
				}
			},500,function(){
				if (node.reachedNode.length>0){
					for (var i=0; i<node.reachedNode.length; i++){
						var nodeToRemove=nodeToAnimate.indexOf(node.reachedNode[i]);
						delete nodeToAnimate[nodeToRemove];
						
						var node_=nodeArray[node.reachedNode[i]];
						animate(node_);
					}
				}
				else{
					animate();
				}
			});
		}
		else{
			animate();
		}
			
		
	}
	function animateANode(node){
		if (node==undefined){
			return;
		}
		
		if (node.leavingEdge.length!=0){
			Animator.setAnimation(function(pc){
				pc=-Math.cos(pc*Math.PI)/2+0.5;
				for (var i=0; i<node.leavingEdge.length; i++){
					var edge=edgeArray[node.leavingEdge[i]];
					edge.pc=pc;
					changes=true;
				}
			},500,function(){
				if (node.reachedNode.length>0){
					for (var i=0; i<node.reachedNode.length; i++){
						animateANode(nodeArray[node.reachedNode[i]]);
					}
				}
			});
		}
	}
	function animateQuick(node){		
		if (node.leavingEdge.length!=0){
			Animator.setAnimation(function(pc){
				pc=-Math.cos(pc*Math.PI)/2+0.5;
				for (var i=0; i<node.leavingEdge.length; i++){
					var edge=edgeArray[node.leavingEdge[i]];
					edge.pc=pc;
					changes=true;
				}
			},700,function(){
				setTimeout(function(){
					node.animate=false;
				},400);
			});
		}
	}
	function drawEdge(edge){
		var pc=edge.pc;
		var f=edge.bezier;
		var pcWidth=(edge.strokeWidth-minWidth)/(maxWidth-minWidth);
		//ctx.globalAlpha=pcWidth*0.75+0.25;
		var defColor=[139,30,43];
		var bgColor=[240,240,240];
		ctx.lineWidth=pcWidth*2*lineWidthCoeff/(zoom/2);
		//ctx.strokeStyle="#8b1e2b";
		//ctx.fillStyle="#8b1e2b";
		var color=colorAlpha(defColor, bgColor, pcWidth*0.75+0.25);
		ctx.strokeStyle=color;
		ctx.fillStyle=color;
		if (mouseOverOneNode){
			var defColor=[195,149,155];
			/*ctx.globalAlpha=0.3;
			ctx.strokeStyle="#c3959b";
			ctx.fillStyle="#c3959b";*/
			var color=colorAlpha(defColor, bgColor, globalAlpha);
			ctx.strokeStyle=color;
			ctx.fillStyle=color;
		}
		if (edge.parentMouseOver){
			var defColor=[180,42,59];
			//ctx.globalAlpha=1;
			ctx.lineWidth=pcWidth*3*lineWidthCoeff/(zoom/2);
			//ctx.strokeStyle="#180";
			//ctx.fillStyle="#b42a3b";
			var color=colorAlpha(defColor, bgColor, 1);
			ctx.strokeStyle=color;
			ctx.fillStyle=color;
		}
		ctx.beginPath();
		var start=f(0);
		ctx.moveTo(start.x,start.y);
		var nbPoint_=(edge.curveLength-minLength)/(maxLength-minLength)*(nbPoint-nbPointSmall)+nbPointSmall;
		nbPoint_=Math.round(nbPoint*Math.min(3,zoom));
		for (var i=1; i<=nbPoint_; i++){
			var point=f(i/nbPoint_*pc);
			ctx.lineTo(point.x,point.y);
		}
		ctx.stroke();
		
		if (zoom>2 && pc==1){
			ctx.beginPath();
			ctx.moveTo(edge.points[0].x,edge.points[0].y);
			for (var i=0; i<edge.points.length; i++){
				ctx.lineTo(edge.points[i].x,edge.points[i].y);
			}
			ctx.fill();
		}
		
		
	}
	function produceVector(f){
		return function(pc){
			return new V(f(pc));
		}
	}
	function setUpInput(){
		$("#canvas, .node").mousedown(function(e){
			previousDraggingPoint=new V(e.pageX, e.pageY);
			mouseDown=true;
			$("body").addClass("grabbing");
		});
		$(window).mouseup(function(){
			mouseDown=false;
			$("body").removeClass("grabbing");
		});
		$(window).mousemove(function(e){
			if (mouseDown){
				var position=new V(e.pageX, e.pageY);
				var move=position.sub(previousDraggingPoint);
				translate=translate.add(move);
				previousDraggingPoint=position;
				changes=true;
			}
		});
		$("#canvas, .node").on('mousewheel', function(e) {
			var newZoom=Math.max(0.5,zoom+e.deltaY/10);
			var zoomDiff=newZoom/zoom;
			
			var mousePosition=new V(e.pageX, e.pageY-65);
			var newPosition=mousePosition.sub(translate).scale(1/zoom).scale(newZoom).add(translate);
			var positionDiff=newPosition.sub(mousePosition);
			
			translate=translate.sub(positionDiff);
			zoom=newZoom;
			changes=true;
		});
	}
	function resize(){
		c.width=c.offsetWidth;
		c.height=c.offsetHeight;
		
		drawingWidth=maxX-minX;
		drawingHeight=maxY-minY;
		drawingRatio=drawingWidth/drawingHeight;
		
		windowWidth=c.width;
		windowHeight=c.height;
		windowRatio=windowWidth/windowHeight;
		
		if (drawingRatio>windowRatio){
			preScale=windowWidth/drawingWidth;
		}
		else{
			preScale=windowHeight/drawingHeight;
		}
		
		preScale*=0.9;
		
		translate.x=windowWidth/2-drawingWidth*preScale/2;
		translate.y=windowHeight/2-drawingHeight*preScale/2;
		
		preTranslate=new V(-minX, -minY);
	}
	function draw(){
		if (changes){
			changes=false;
			c.width=c.width;
			
			ctx.translate(translate.x,translate.y);
			ctx.scale(zoom,zoom);
			
			ctx.scale(preScale,preScale);
			ctx.translate(preTranslate.x,preTranslate.y);
			
			ctx.lineWidth=lineWidthCoeff/(zoom/2);
			//ctx.lineCap="round";
			
			for (var i=0; i<edgeArray.length; i++){
				drawEdge(edgeArray[i]);
			}
			for (var i=0; i<nodeArray.length; i++){
				drawNode(nodeArray[i]);
			}
		}
		
		requestAnimationFrame(draw);
	}
	
	function colorAlpha(color1, colorBg, alpha){
		/*var componentToHex=function(c) {
			var hex = c.toString(16);
			return hex.length == 1 ? "0" + hex : hex;
		}*/
		
		var r=Math.round(color1[0]*alpha+colorBg[0]*(1-alpha));
		var g=Math.round(color1[1]*alpha+colorBg[1]*(1-alpha));
		var b=Math.round(color1[2]*alpha+colorBg[2]*(1-alpha));
		
		return "rgb("+r+","+g+","+b+")";
	}
	
	function drawNode(node){
		var position=node.position.add(preTranslate).scale(preScale).scale(zoom).add(translate);
		var smallSize=(new V(200,36)).scale(preScale).scale(zoom);
		var singleSize=(new V(250,36)).scale(preScale).scale(zoom);
		var bigSize=(new V(75,75));
		
		if (node.mouseOver || smallSize.x>=bigSize.x){
			var size=bigSize;
			node.nodeElem.addClass("full");
		}
		else{
			var size=smallSize;
			node.nodeElem.removeClass("full");
			
			if (node.parentMouseOver){
				size=size.scale(1.5);
			}
		}
		
		if (node.single){
			size=singleSize;
		}
		
		if (position.x>windowWidth || position.y>windowHeight || 
			position.x+size.x<0 || position.y+size.y<0){
				node.nodeElem.addClass("outOfScreen");
		}
		else{
			node.nodeElem.removeClass("outOfScreen");
			node.nodeElem.css({
				left:Math.round(position.x-size.x/2)+"px",
				top:Math.round(position.y-size.y/2)+"px",
				width:Math.round(size.x)+"px",
				height:Math.round(size.y)+"px"
			});
		}
	}
	function cubic(p0,p1,p2,p3){
		return function(t){
			var t0=p0.scale((1-t)*(1-t)*(1-t));
			var t1=p1.scale((1-t)*(1-t)*t*3);
			var t2=p2.scale((1-t)*t*t*3);
			var t3=p3.scale(t*t*t);
			return t0.add(t1).add(t2).add(t3);
		}
	}
	function multipleBezier(beziers){
		var totalLength=0;
		var lengthArray=[];
		for (var i=0; i<beziers.length; i++){
			var l=lineLength(beziers[i]);
			lengthArray.push(l);
			totalLength+=l;
		}
		return function(t){
			var scaledT=t*totalLength;
			var added=0;
			for (var i=0; i<lengthArray.length; i++){
				start=added;
				added+=lengthArray[i];
				if (scaledT<=added){
					return beziers[i]((scaledT-start)/(added-start));
				}
			}
			return beziers[beziers.length-1](1);
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
			total+=v2.sub(v1).length();
		}
		return total;
	}
	function extractNodes(){
		var index=0;
		$("g[class='node']",xml).each(function(){
			var node=$(this);
			var textNode=$("text",node);
			var title=textNode.text();
			//console.log(title);
			var x=textNode.attr("x")*1*prePreScale.x;
			var y=textNode.attr("y")*1*prePreScale.y;
			
			minX=Math.min(minX,x);
			minY=Math.min(minY,y);
			maxX=Math.max(maxX,x);
			maxY=Math.max(maxY,y);
			
			var position=new V(x,y);
			var words=title.split(" ");
			var nodeElem=getNodeElem(words, position);
			
			var node=new TextNode(position, title, index, nodeElem);
			
			if (words.length=="1"){
				node.single=true;
			}
			
			nodeElem.mouseenter(function(){
				changes=true;
				node.mouseOver=true;
				mouseOverOneNode=true;
				Animator.setAnimation(function(pc){
					globalAlpha=(1-pc)*0.7+0.3;
				},500);
				$("body").addClass("mouseOverOneNode");
				node.nodeElem.addClass("transitionning");
				for (var i=0; i<node.leavingEdge.length; i++){
					edgeArray[node.leavingEdge[i]].parentMouseOver=true;
				}
				for (var i=0; i<node.reachedNode.length; i++){
					nodeArray[node.reachedNode[i]].parentMouseOver=true;
					nodeArray[node.reachedNode[i]].nodeElem.addClass("parentMouseOver");
				}
				if (!node.animate){
					animateQuick(node);
					node.animate=true;
				}
			});
			nodeElem.mouseleave(function(){
				changes=true;
				node.mouseOver=false;
				mouseOverOneNode=false;
				Animator.setAnimation(function(pc){
					globalAlpha=(pc)*0.7+0.3;
				},500);
				setTimeout(function(){
					node.nodeElem.removeClass("transitionning");
				},500);
				$("body").removeClass("mouseOverOneNode");
				for (var i=0; i<node.leavingEdge.length; i++){
					edgeArray[node.leavingEdge[i]].parentMouseOver=false;
				}
				for (var i=0; i<node.reachedNode.length; i++){
					nodeArray[node.reachedNode[i]].parentMouseOver=false;
					nodeArray[node.reachedNode[i]].nodeElem.removeClass("parentMouseOver");
				}
			});
			nodeElem.click(function(){
				animateANode(node);
			})
			
			nodeArray.push(node);
			nodeToAnimate.push(index);
			$("#nodes").append(nodeElem);
			index++;
		});
	}
	function getNodeElem(words){
		if (words.length==1){
			var htmlString="<div class='node single'>" +
				words[0] +
				"</div>";
		}
		else{
			var htmlString=
				"<div class='node'>" +
				"<table class='themeTable'>" +
					"<tr>" +
						"<th>#</th>" +
						"<th>Word</th>" +
					"</tr>";

			for (var i=0; i<(words.length-1 || words.length); i++){
				htmlString+=wordString(words[i],i);
			}	
				
			htmlString += 
				"</table>" +
				"</div>";
		}
			
		return $(htmlString);
	}
	function wordString(word, index){
		return "<tr>" +
				"<td>"+(index+1)+"</td>" +
				"<td>"+word+"</td>" +
			"</tr>";
	}
	function extractEdges(){
		$("g[class='edge']",xml).each(function(){
			var node=$(this);
			
			var pathNode=$("path",node);
			var pathString=pathNode.attr("d");
			var path=svgPathParser.parse(pathString);
			
			for (var i=1; i<path.length; i++){
				path[i].x0=path[i-1].x;
				path[i].y0=path[i-1].y;
			}
			
			//var bezier=getBezier(path.slice(1));
			var beziersArray=[];
			for (var i=1; i<path.length; i++){
				var f=cubic(
					(new V(path[i].x0,path[i].y0)).scaleM(prePreScale),
					(new V(path[i].x1,path[i].y1)).scaleM(prePreScale),
					(new V(path[i].x2,path[i].y2)).scaleM(prePreScale),
					(new V(path[i].x,path[i].y)).scaleM(prePreScale)
				);
				beziersArray.push(f);
			}
			var bezier=multipleBezier(beziersArray);
			var curveLength=lineLength(bezier);
			maxLength=Math.max(maxLength,curveLength);
			minLength=Math.min(minLength,curveLength);
			
			var strokeWidth=pathNode.attr("stroke-width")*1 || 1;
			
			maxWidth=Math.max(maxWidth,strokeWidth);
			minWidth=Math.min(minWidth,strokeWidth);
			
			var pointsNode=$("polygon",node);
			var pointsString=pointsNode.attr("points");
			var points=[];
			var pointsStringExploded=pointsString.split(" ");
			for (var i=0; i<pointsStringExploded.length; i++){
				var p=pointsStringExploded[i].split(",");
				points.push((new V(p[0]*1,p[1]*1)).scaleM(prePreScale));
			}
			
			var firstMove=path[0];
			var start=(new V(firstMove["x"], firstMove["y"])).scaleM(prePreScale);
			var end=points[0];
			
			edgeArray.push(new Edge(start, end, path, points, strokeWidth, bezier, curveLength));
		});
	}
	function TextNode(position, text, index, nodeElem){
		this.position=position;
		this.text=text;
		this.index=index;
		this.nodeElem=nodeElem;
		this.leavingEdge=[];
		this.reachedNode=[];
		this.mouseOver=false;
		this.parentMouseOver=false;
	}
	function matchBeginingEnd(){
		var nodeArrayPosition=[];
		for (var i=0; i<nodeArray.length; i++){
			var position=nodeArray[i].position;
			var index=nodeArray[i].index;
			nodeArrayPosition.push({
				x:position.x,
				y:position.y,
				index:index
			});
		}
		var tree=new kdTree(
			nodeArrayPosition, 
			function(a, b){
				return (a.x - b.x)*(a.x - b.x) +  (a.y - b.y)*(a.y - b.y);
			},
			["x", "y"]
		);
		for (var i=0; i<edgeArray.length; i++){
			var edge=edgeArray[i];
			var startNode=tree.nearest(edge.start, 1)[0][0];
			var endNode=tree.nearest(edge.end, 1)[0][0];
			
			//console.log(nodeArray[startNode["index"]].text+" -> "+nodeArray[endNode["index"]].text)
			
			nodeArray[startNode["index"]].leavingEdge.push(i);
			nodeArray[startNode["index"]].reachedNode.push(endNode["index"]);
		}
	}
	function Edge(start, end, path, points, strokeWidth, bezier, curveLength){
		this.start=start;
		this.end=end;
		this.path=path;
		this.points=points;
		this.strokeWidth=strokeWidth;
		this.bezier=bezier;
		this.parentMouseOver=false;
		this.pc=1;
		this.curveLength=curveLength;
	}
	function V(x,y){
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
			return new V(
				v_.x-v2.x,
				v_.y-v2.y
			);
		}
		this.add=function(v2){
			return new V(
				v_.x+v2.x,
				v_.y+v2.y
			);
		}
		this.scale=function(s){
			return new V(
				v_.x*s,
				v_.y*s
			);
		}
		this.scaleM=function(v2){
			return new V(
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
		font-family:din_lightregular;
		font-size:13px;
		margin:0;
		padding:0;
		background-color:#f0f0f0;
		padding-top:70px;
	}
	body.grabbing{
		cursor: grabbing;
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
	#home{
		height:17px;
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
		position:absolute;
		top:65px;
		bottom:0;
		left:0;
		width:100%;

		text-align:justify;
		overflow: hidden;
	}
	#canvas{
		position:absolute;
		top:0;
		left:0;
		width:100%;
		height:100%;
	}
	
	.node{
		position:absolute;
		z-index:2;
		background-color:white;
		border-radius:10px;
		
		font-size:10px;
		cursor:default;
		width:0;
		height:0;
		
		-webkit-transition: opacity .5s;
		-moz-transition: opacity .5s;
		-o-transition: opacity .5s;
		transition: opacity .5s;
	}
	.node.transitionning{
		-webkit-transition: width .2s, height .5s;
		-moz-transition:  width .2s, height .5s;
		-o-transition:  width .2s, height .5s;
		transition:  width .2s, height .5s;
	}
	.node.outOfScreen{
		display:none;
	}
	.mouseOverOneNode .node{
		opacity:0.3;
	}
	.mouseOverOneNode .node.parentMouseOver, .mouseOverOneNode .node:hover{
		opacity:1;
	}
	.node:hover{
		z-index:3;
		
	}
	.node.full{
		padding:5px;
		padding-left:15px;
		background-color:rgba(255,255,255,0.3);
		width:75px !important;
		height:75px !important;
		box-shadow: 0px 0px 5px rgba(0,0,0,0.1);
		
	}
	.node.full:hover{
		background-color:rgba(255,255,255,0.9);
		box-shadow: 0px 0px 5px rgba(0,0,0,0.4);
		
		/*-webkit-transition: left .2s, top .5s, opacity 1s;
		-moz-transition:  left .2s, top .5s, opacity 1s;
		-o-transition:  left .2s, top .5s, opacity 1s;
		transition:  left .2s, top .5s, opacity 1s;*/
	}
	.node.parentMouseOver{
		background-color:rgba(255,255,255,0.7);
		box-shadow: 0px 0px 5px rgba(0,0,0,0.4);
	}
	.node table{
		display:none;
	}
	.node.full table{
		display:inline;
	}
	.node.single{
		font-size:10px;
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
</style>
<title></title>
</head>
<body onload="load();">
	<div id="menu" class="margin">
		<a class="menuEntry title" href="./"><img id="home" src="images/home.png"></a>
		<a class="menuEntry"  href="./enter.php">Event Stream Detection</a>
		<a class="menuEntry" href="./themes.php">Browse themes</a>
		<a class="menuEntry selected" href="./graph.php">Explore graph</a>
		<span class="menuEntry search"><input type="text" value="Search themes"></span>
	</div>
	<div id="content" class="margin">
		<canvas id="canvas"></canvas>
		<div id="nodes"></div>
	</div>
</body>
</html>
