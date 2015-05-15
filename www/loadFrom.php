<?php

if (isset($_GET["from"]) && $_GET["from"]*1 >= 0){
	$from=$_GET["from"]*1;
	
	/*$dico=file_get_contents("liste_francais.txt");
	$dic=explode("\n",$dico);
	$size=count($dic);

	$theme=array();

	for ($i=0; $i<100; $i++){
		$theme[$i]=array(
			"i"=>$from+$i,
			"wl"=>array()
		);
		for ($j=0; $j<20; $j++){
			$theme[$i]["wl"][$j]=array(
				"w"=>$dic[rand(0,$size-1)],
				"p"=>rand(0,10000)/10001
			);
		}
	}

	*/
	
	$theme=array();
	
	for ($i=0; $i<50; $i++){
		if (file_exists("./data/themes/theme".($from+$i).".json")){
			$theme[$i]=json_decode(file_get_contents("./data/themes/theme".($from+$i).".json"));
		}
	}
	
	print(json_encode($theme));
	
	//print(json_last_error_msg());
}

?>