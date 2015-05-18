<!DOCTYPE html> 
<html>
<head>
<meta name="viewport" content="width=1020px" />
<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
<link rel="shortcut icon" href="images/favicon.ico" />
<script type="text/javascript" src="../jquery-1.9.0.min.js"></script>
<script type="text/javascript" src="../jquery.mousewheel.min.js"></script>
<script type="text/javascript" src="svgPathParser.js"></script>
<script type="text/javascript" src="kdTree-min.js"></script>
<script type="text/javascript" src="../bezier.js"></script>
<script type="text/javascript">
	window.requestAnimationFrame=window.requestAnimationFrame || window.mozRequestAnimationFrame || window.webkitRequestAnimationFrame || window.msRequestAnimationFrame || function(f){setTimeout(f,1);};
	var svg='<?php print(str_replace("\n","",file_get_contents("graph.svg"))); ?>';
	var nodeArray=[];
	var edgeArray=[];
	var maxX=Number.MIN_VALUE; 
	var maxY=Number.MIN_VALUE; 
	var minX=Number.MAX_VALUE; 
	var minY=Number.MAX_VALUE; 
	var maxWidth=Number.MIN_VALUE;
	var minWidth=Number.MAX_VALUE;
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
	var lineWidthCoeff=10;
	var changes=true;
	var nbPoint=30;
	var prePreScale;
	function load(){
		var xmlDoc = $.parseXML(svg);
		xml = $(xmlDoc);
		prePreScale=new V(1,1.5);
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
	function drawEdge(edge,pc){
		var f=edge.bezier;
		var pcWidth=(edge.strokeWidth-minWidth)/(maxWidth-minWidth);
		ctx.globalAlpha=pcWidth*0.75+0.25;
		ctx.lineWidth=pcWidth*2*lineWidthCoeff/(zoom/2);
		ctx.beginPath();
		var start=f(0);
		ctx.moveTo(start.x,start.y);
		for (var i=1; i<=nbPoint; i++){
			var point=f(i/nbPoint*pc);
			ctx.lineTo(point.x,point.y);
		}
		ctx.stroke();
	}
	function produceVector(f){
		return function(pc){
			return new V(f(pc));
		}
	}
	function setUpInput(){
		$(window).mousedown(function(e){
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
		$(window).on('mousewheel', function(e) {
			var newZoom=Math.max(1,zoom+e.deltaY/10);
			var zoomDiff=newZoom/zoom;
			
			var mousePosition=new V(e.pageX, e.pageY);
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
		
		windowWidth=$(window).width();
		windowHeight=$(window).height();
		windowRatio=windowWidth/windowHeight;
		
		if (drawingRatio>windowRatio){
			preScale=windowWidth/drawingWidth;
			translate.y=windowHeight/2-drawingHeight*preScale/2;
			translate.x+=30;
		}
		else{
			preScale=windowHeight/drawingHeight;
			translate.x=windowWidth/2-drawingWidth*preScale/2;
		}
		
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
			ctx.lineCap="round";
			ctx.strokeStyle="#8b1e2b";
			
			for (var i=0; i<edgeArray.length; i++){
				drawEdge(edgeArray[i], 1);
			}
		}
		
		requestAnimationFrame(draw);
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
			var x=textNode.attr("x")*1*prePreScale.x;
			var y=textNode.attr("y")*1*prePreScale.y;
			
			minX=Math.min(minX,x);
			minY=Math.min(minY,y);
			maxX=Math.max(maxX,x);
			maxY=Math.max(maxY,y);
			
			var position=new V(x,y);
			nodeArray.push(new TextNode(position, title, index));
			index++;
		});
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
			var start=new V(firstMove["x"], firstMove["y"]);
			var end=points[0];
			
			edgeArray.push(new Edge(start, end, path, points, strokeWidth, bezier));
		});
	}
	function TextNode(position, text, index){
		this.position=position;
		this.text=text;
		this.index=index;
		this.leavingEdge=[];
		this.reachedNode=[];
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
			
			nodeArray[startNode.index].leavingEdge.push(i);
			nodeArray[startNode.index].reachedNode.push(endNode["index"]);
		}
	}
	function Edge(start, end, path, points, strokeWidth, bezier){
		this.start=start;
		this.end=end;
		this.path=path;
		this.points=points;
		this.strokeWidth=strokeWidth;
		this.bezier=bezier;
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
	body{
		
	}
	body.grabbing{
		cursor: grabbing;
	}
	#canvas{
		position:absolute;
		top:0;
		left:0;
		width:100%;
		height:100%;
	}
</style>
<title></title>
</head>
<body onload="load();">
	<canvas id="canvas"></canvas>
</body>
</html>
