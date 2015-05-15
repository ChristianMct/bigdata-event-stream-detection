<?php
	if(isset($_GET["from"]) && isset($_GET["to"]) && isset($_GET["themeIndex"])){
		$timeFrom = strtotime($_GET["from"]);
		$timeTo = strtotime($_GET["to"]);
		
		$oneDay=60*60*24;
		$twoDays=2*$oneDay;
		$threeDays=3*$oneDay;
		$oneWeek=$oneDay*7;
		$twoWeeks=2*$oneWeek;
		$oneMonth=$oneDay*30;
		$threeMonths=$oneMonth*3;
		$sixMonths=$oneMonth*6;
		$oneYear=$oneDay*365;
		$threeYears=$oneYear*3;
		$tenYears=$oneYear*10;
		
		$maxNumberOfValue=30;
		
		$requestedInterval=$timeTo-$timeFrom;
		
		$window=null;
		$periodString="";
		
		if ($oneDay*$maxNumberOfValue > $requestedInterval){
			$window=$oneDay;
			$periodString="one day";
		}
		else if ($twoDays*$maxNumberOfValue > $requestedInterval){
			$window=$twoDays;
			$periodString="two days";
		}
		else if ($threeDays*$maxNumberOfValue > $requestedInterval){
			$window=$threeDays;
			$periodString="three days";
		}
		else if ($oneWeek*$maxNumberOfValue > $requestedInterval){
			$window=$oneWeek;
			$periodString="one week";
		}
		else if ($twoWeeks*$maxNumberOfValue > $requestedInterval){
			$window=$twoWeeks;
			$periodString="two weeks";
		}
		else if ($oneMonth*$maxNumberOfValue > $requestedInterval){
			$window=$oneMonth;
			$periodString="one months";
		}
		else if ($threeMonths*$maxNumberOfValue > $requestedInterval){
			$window=$threeMonths;
			$periodString="three months";
		}
		else if ($sixMonths*$maxNumberOfValue > $requestedInterval){
			$window=$sixMonths;
			$periodString="six months";
		}
		else if ($oneYear*$maxNumberOfValue > $requestedInterval){
			$window=$oneYear;
			$periodString="one year";
		}
		else if ($threeYears*$maxNumberOfValue > $requestedInterval){
			$window=$threeYears;
			$periodString="three years";
		}
		else{
			$window=$tenYears;
			$periodString="ten years";
		}
		
		//print("window ".$window."\n");
		//print("requested interval ".$requestedInterval."\n");
		
		$themeRawData=file_get_contents("./data/themesData/theme".$_GET["themeIndex"].".json");
		$themeLinesData=explode("\n",$themeRawData);
		
		$themeInterestingData=array();
		$nbLines=count($themeLinesData);
		
		$k=0;
		$innerIterator=0;
		do{
			//print($themeLinesData[$k]."\n");
			$lineExploded=explode(",", $themeLinesData[$k]);
			$lastTimeStamp=$lineExploded[0];
			$strength=$lineExploded[1];
			
			if ($lastTimeStamp >= $timeFrom){
				$themeInterestingData[$innerIterator]=$lineExploded;
				$innerIterator++;
			}
			
			$k++;
		}
		while($lastTimeStamp < $timeTo && $k<$nbLines-1);
		
		$finalResult=array(
			"period"=>$periodString,
			"contentDate"=>array(),
			"contentData"=>array()
		);
		$nbEntry=count($themeInterestingData);
		$lastStart=$timeFrom;
		$accumulator=0;
		$nbEntryInAccumulator=0;
		$finalEntry=0;
		for ($i=0; $i<$nbEntry; $i++){
			if ($window==$oneDay){
				$lineExploded=$themeInterestingData[$i];
				$lastTimeStamp=$lineExploded[0];
				$strength=$lineExploded[1];
				$finalResult["contentDate"][$finalEntry]=date("m/d/Y",$lastTimeStamp);
				$finalResult["contentData"][$finalEntry]=$strength;
				$finalEntry++;
			}
			else{
				$lineExploded=$themeInterestingData[$i];
				$lastTimeStamp=$lineExploded[0];
				$strength=$lineExploded[1];
				if ($lastTimeStamp<$lastStart+$window){
					$accumulator+=$strength;
					$nbEntryInAccumulator++;
				}
				else{
					$finalResult["contentDate"][$finalEntry]=date("m/d/Y",$lastStart);
					$finalResult["contentData"][$finalEntry]=$accumulator/$nbEntryInAccumulator;
					$finalEntry++;
					$accumulator=0;
					$nbEntryInAccumulator=0;
					$lastStart+=$window;
				}
			}
		}
		
		print(json_encode($finalResult));
		
		
	}

?>
