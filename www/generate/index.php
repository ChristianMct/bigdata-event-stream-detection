<?php

	if (isset($_POST["theme"])){
		
		$themeDataThere=isset($_POST["themeData"]) && trim($_POST["themeData"])!="";
		
		if ($themeDataThere){
		
			$themeData=$_POST["themeData"];
			$themeData=trim($themeData);
			$themesDataArray=explode("\n",$themeData);
			$data=array();
			$themeDataStart=explode(",", $themesDataArray[0])[0]/1000;
			$themeDataStart=date("m/d/Y",$themeDataStart);
			$lastIndex=count($themesDataArray)-1;
			$themeDataEnd=explode(",", $themesDataArray[$lastIndex])[0]/1000;
			$themeDataEnd=date("m/d/Y",$themeDataEnd);
			
			$str="";
			
			foreach ($themesDataArray as $key => $value){
				$ex=explode(",",$value);
				$timeStamp=$ex[0]/1000;
				$strength=$ex[1];
				$str.=$timeStamp.",".$strength."\n";
			}
			
			
		}
		else{
			$themeDataStart="01/01/1808";
			$themeDataEnd="12/31/1998";
		}
		
		
		
		$themeString=$_POST["theme"];
		
		$themeString=trim($themeString);
		$themeString=substr($themeString, 0, -1);
		$themeString=substr($themeString, 1);
		
		$themeString=str_replace(" ","",$themeString);
		$themeString=str_replace("\n","",$themeString);
		$themeString=str_replace("\t","",$themeString);
		
		$themesExploded=explode(",", $themeString);
		
		$nmbTheme=count(scandir("../data/themes"))-2;
		
		$theme=array(
			"i"=>$nmbTheme,
			"start"=>$themeDataStart,
			"end"=>$themeDataEnd,
			"noData"=>$themeDataThere?"false":"true",
			"wl"=>array()
		);
		
		foreach ($themesExploded as $key => $value){
			$theme_=explode("=",$value);
			$theme["wl"][]=array(
				"w"=>$theme_[0],
				"p"=>$theme_[1]*1
			);
			
		}
		
		file_put_contents("../data/themes/theme".$nmbTheme.".json", json_encode($theme));
		
		if ($themeDataThere){
			file_put_contents("../data/themesData/theme".$nmbTheme.".json", $str);
		}

		exit;
	}
	

?>

<!DOCTYPE html> 
<html>
<head>
<meta name="viewport" content="width=1020px" />
<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
<link rel="shortcut icon" href="images/favicon.ico" />
<script type="text/javascript">
	function load(){
		
	}
</script>
<style type="text/css">
	@font-face {
		font-family: 'din';
		src: url('../font/din/din.eot');
		src: url('../font/din/din.eot?#iefix') format('embedded-opentype'),
			 url('../font/din/din.woff') format('woff'),
			 url('../font/din/din.ttf') format('truetype'),
			 url('../font/din/din.svg#din') format('svg');
		font-weight: normal;
		font-style: normal;
	}
	body{
		font-family:din;
		margin-right:100px;
		margin-left:100px;
		text-align:justify;
	}
	textarea{
		width:40%;
		height:100px;
	}
</style>
<title></title>
</head>
<body onload="load();">
	<h1>Add Theme</h1>
	<form method="POST" action="./index.php">
		Theme words<br>
		Ex:<small><pre>{neutres=0.027485606851249497, blocus=0.02028598195769603, représailles=0.019941174424969427,
droit=0.01627506051491361, neutre=0.014853251656979472, 
marine=0.013896108903107551, allemagne=0.013194209096074957, navires=0.011585210162075853, 
tonnes=0.01148516293671579, reich=0.011052739751545282, allemands=0.01094989063478517, 
belligérant=0.010250845964402861, marins=0.009394153255523485}</pre></small><br>
		<textarea name="theme"></textarea><br><br><br>
		Theme data<br>
		Ex:<small><pre>-948286800000,25
-948200400000,15
-948114000000,13
-948027600000,5
-947941200000,15
-947854800000,17
-947768400000,20
-947682000000,26
-947595600000,20
-947509200000,14
-947422800000,1
-947336400000,10
-947163600000,15
-947077200000,16
-946990800000,11
-946904400000,12
-946818000000,11
-946731600000,3</pre></small><br>
		<textarea name="themeData"></textarea><br><br>
		<input type="submit" value="Submit">
	</form>
</body>
</html>
