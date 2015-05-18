<?php
	
	$dico=file_get_contents("liste_francais.txt");
	$dic=explode("\n",$dico);
	$size=count($dic);
	
	for ($i=0; $i<300; $i++){
		$theme=array(
			"i"=>$i,
			"start"=>"01/01/1808",
			"end"=>"12/31/1998",
			"noData"=>rand(0,1)==0?"true":"false",
			"wl"=>array()
		);
		$prob=array();
		for ($j=0; $j<15; $j++){
			$prob[$j]=rand(0,10000)/10001;
		}
		rsort($prob);
		for ($j=0; $j<15; $j++){
			$theme["wl"][$j]=array(
				"w"=>$dic[rand(0,$size-1)],
				"p"=>$prob[$j]
			);
		}
		
		$themeData=array();
		$oneDay=60*60*24;
		$timeFrom = strtotime("01/01/1800");
		$timeTo = strtotime("12/31/1998");
		
		$themeDataString="";
		
		$k=0;
		for ($j=$timeFrom; $j<$timeTo; $j+=$oneDay){
			/*$themeData[$k]=array(
				"t"=>$j,
				"s"=>rand(0,30)
			);*/
			$themeDataString.=$j.",".rand(0,30)."\n";
			$k++;
		}
		
		
		file_put_contents("./data/themes/theme".$i.".json", json_encode($theme));
		file_put_contents("./data/themesData/theme".$i.".json", $themeDataString);
	}

?>