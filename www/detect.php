<!DOCTYPE html> 
<html>
<head>
<meta name="viewport" content="width=1020px" />
<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
<link rel="shortcut icon" href="images/favicon.ico" />
<script type="text/javascript">
	function load(){
		var c=document.getElementById("canvas");
		var ctx=c.getContext("2d");
		
		var img=document.getElementById("logo");
		
		c.width=img.offsetWidth;
		c.height=img.offsetHeight;
		
		ctx.drawImage(img,0,0);
		
		var imgData=ctx.getImageData(0,0,c.width,c.height);
		
		var data=[];
		
		for (var y=0; y<c.height; y++){
			data[y]=[0];
			var previous=0;
			for (var x=0; x<c.width; x++){
				var opacity=imgData.data[(y*c.width+x)*4+3];
				//console.log(opacity);
				if ((previous==0 && opacity==255) || (previous==255 && opacity==0)){
					previous=opacity;
					data[y].push(x/c.width);
				}
			}
			if (previous==255){
				data[y].push(1);
			}
		}
		
		console.log(JSON.stringify(data));
	}
</script>
<style type="text/css">
	body{
		
	}
</style>
<title></title>
</head>
<body onload="load();">
	<canvas id="canvas"></canvas>
	<img id="logo" src="images/logo_.png">
</body>
</html>
