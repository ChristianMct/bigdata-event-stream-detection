<?php
	$string=file_get_contents("Theme.txt");
	$exploded=json_decode($string);
	
	for ($i=0; $i<count($exploded); $i++){
		$sub=$exploded[$i];
		print($sub."\n\n");
		$keyVal=json_decode($sub,true);
		$theme=array(
			"i"=>$i,
			"start"=>"01/01/1808",
			"end"=>"12/31/1998",
			"noData"=>"true",
			"wl"=>array()
		);
		$k=0;
		foreach ($keyVal as $key => $value){
			$theme["wl"][$k]=array(
				"w"=>$key,
				"p"=>$value
			);
			$k++;
		}
		file_put_contents("./data/themes/theme".$i.".json", json_encode($theme));
	}
?>