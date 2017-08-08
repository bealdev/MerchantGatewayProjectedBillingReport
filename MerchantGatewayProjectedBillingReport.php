<?php

class ProjectedBillingTable_MerchantGateway extends MysqlQuery
{
	function __construct()
	{
		$server = Application::getInstance()->getClientServer();
		
		$sql = "
SELECT
	C.campaignName,
	C.campaignId,
	P.price,
	P.shippingPrice,
	P.salesTax,
	P.productId,
	P.purchaseId,
	P.nextBillDate,	
	P.billingCycleNumber,
	P.billingIntervalDays,
	P.finalBillingCycle,
	P.trialEnabled,
	P.billerId,
	P.billingCycleType,
	P.trialType,
	B.title as billerName,
	WC.currencySymbol,
	CASE WHEN billingCycleNumber < 4 THEN CONCAT(P.billingCycleNumber,'-',P.billerId)
		ELSE CONCAT('4-',P.billerId) END as groupKey
FROM recurring_purchases P
INNER JOIN orders O ON O.orderId = P.orderId
INNER JOIN campaigns C ON C.campaignId = O.campaignId
INNER JOIN billers B ON B.billerId = P.billerId
LEFT JOIN konnek.world_currencies WC ON WC.currencyCode = C.currency
WHERE P.status IN('ACTIVE','TRIAL','RECYCLE_BILLING')
ORDER BY P.nextBillDate
";
			
		parent::__construct($server, $sql);
		
		$match_rules = array('affiliateId'=>function($query,$params)
							 {
								if(!empty($params->affiliateId))
								{
									$query->where("O.sourceId = ?",(int) $params->affiliateId); 
								}
							  },
							  'callCenterId'=>function($query,$params)
							  {
								if(!empty($params->callCenterId))
								{
									$query->where("O.sourceId = ?",(int) $params->callCenterId); 
								}
							  });
							 
		$this->matchDefs = $match_rules;
		$this->dateRanges = array('nextBillDate');
	}
	
	function getResult($args)
	{
		$this->startDate = $args->startDate;
		$args->resultsPerPage = '*';
		
		$result = parent::getResult($args);

		$base_row = array();

		$data = array();
		
		$attritionRates = array(1=>$this->queryArgs->cycle1Attrition,
								2=>$this->queryArgs->cycle2Attrition,
								3=>$this->queryArgs->cycle3Attrition,
								4=>$this->queryArgs->cycle4PlusAttrition);
							
		
		$start = new DateTime('tomorrow');
		$end = $this->queryArgs->nextBillDate[1];
		
		$base_weeks = array();
		$base_result = array('count'=>0,'rev'=>0);
		$weeks_index = array();
		
		$totals = array();
		
		while($start <= $end)
		{
			$bow = clone $start;
			$start->modify("+6 days");
			$eow = clone $start;
			
			if($eow > $end)
				$eow = clone $end;
			
			$key = $bow->format("M j").' - '.$eow->format("M j");
			$start->modify("+1 days");
			$base_weeks[$key] = $base_result;
			$weeks_index[$bow->getTimeStamp()] = $key;
		}
		
		$endDate = $end;
		$totals = $base_weeks;
		
		$data = array();
		
		$rs = arrays::groupByKey($result->data,'groupKey');
		
		ksort($rs);
		
		foreach($rs as $group)
		{
			$group = arrays::indexByKey($group,'purchaseId');
			arrays::reformRows($group,function($row) use ($attritionRates,$endDate,&$rs)
			{
				extract((array) $row);
				
				if(!empty($hasParent))
					return;
				
				$nextBillDate = new DateTime($nextBillDate);
				$end = clone $endDate;		
				
				$i = 0;
				while($nextBillDate <= $end)
				{
					
					$totalBill = $price + $shippingPrice + $salesTax;
					if($trialType == 'DELAYED' && $trialEnabled && $billingCycleNumber == 2)
					{
						if(!empty($rebillPrice))
							$totalBill = $rebillPrice + $salesTax;
						else
							$totalBill = $price + $salesTax;
					}
					
					$row->totalBill = $totalBill;
					
					$nextBillDate->modify("+{$billingIntervalDays} days");
					$billingCycleNumber++;
					
					if($nextBillDate > $end)
						break;
					
					if(($billingCycleType == 'ONE_TIME' && $billingCycleNumber > 2))
						break;
						
					if($finalBillingCycle > 0 && $billingCycleNumber > $finalBillingCycle)
						break;
					
					$key = ($billingCycleNumber < 4 ? $billingCycleNumber : 4).'-'.$billerId;
					
					$totalBill = $price + $shippingPrice + $salesTax;
					if($trialType == 'DELAYED' && $trialEnabled && $billingCycleNumber == 2)
					{
						if(!empty($rebillPrice))
							$totalBill = $rebillPrice + $salesTax;
						else
							$totalBill = $price + $salesTax;
					}
					if(empty($row->children))
						$row->children = array();
					
					$child = clone $row;
					$child->billingCycleNumber = $billingCycleNumber;
					$child->nextBillDate = $nextBillDate->format("Y-m-d");
					$child->hasParent = true;
					$child->totalBill = $totalBill;
					$child->children = array();
					
					$rs[$key][$purchaseId.$billingCycleNumber] = $child;
					
					foreach($row->children as $ch)
					{
						list($k1,$k2) = explode("|",$ch);
						$rs[$k1][$k2]->children[] = $key.'|'.$purchaseId.$billingCycleNumber;
						
					}
					
					$row->children[] = $key.'|'.$purchaseId.$billingCycleNumber;
					$i++;
				}
			});
		}
	
		foreach($rs as $key=>&$group)
		{
			$total = count($group);
			$cycle = substr($key,0,strrpos($key,'-'));
			$attrition = $attritionRates[$cycle];
			$keep = round($total * ($attrition / 100));
			$remove = $total - $keep;
		
			if($remove == 0)
				continue;
			
			
			$step = ceil($total / $remove);

			$group = array_values($group);
			
			for($i=0,$n=0;$i<$remove;$i++,$n+=$step)
			{	
				if($n > $total - 1)
					$n = arrays::firstKey($group);
				
				$row = $group[$n];
				
				if(!empty($row->children))
				{
					foreach($row->children as $key)
					{
						list($k1,$k2) = explode("|",$key);	
						unset($rs[$k1][$k2]);
					}
				}
				unset($group[$n]);
			}
		}
		unset($group);
		
		foreach($rs as $group)
		{
			
			arrays::reformRows($group, function($row) use(&$data,$base_weeks,$base_result,$weeks_index,&$totals)
			{
				
				$nextBill = strtotime($row->nextBillDate);	

				extract((array) $row);
				
				if(empty($data[$billerId]))
				{
					$data[$billerId]['weeks'] = $base_weeks;
					$data[$billerId]['billerName'] = $billerName;
				}
				
				$weekFound = false;
				
				foreach($weeks_index as $time=>$key)
				{
					$eow = $time + 3600 * 24 * 7;
					if($nextBill >= $time && $nextBill < $eow)
					{
						$data[$billerId]['weeks'][$key]['count']++;
						$data[$billerId]['weeks'][$key]['rev'] += $totalBill;
						$data[$billerId]['weeks'][$key]['cur'] = $currencySymbol;
						
						$totals[$key]['count']++;
						$totals[$key]['rev'] += $totalBill;
						$totals[$key]['cur'] = $currencySymbol;
						
						$weekFound = true;
						break;						
					}
				}
				
				if(empty($weekFound))
				{
					tools::dumpVar($row);
					
						
				}
				
			});
		}
		$data = arrays::sortByKey($data,'billerName');
		
		
		$element = array('billerName'=>"Total",
						  'weeks'=>$totals,
						  'isTotal'=>true);
								  
		$data[] = $element;
		
		$result->data = $data;
		$result->base_weeks = $base_weeks;
		return $result;
		

		foreach($result->data as $row)
		{
			foreach($weeks_index as $time=>$key)
			{
				$nextBill = new DateTime($row->nextBillDate);
				$nextBill = $nextBill->getTimestamp();
				
				$eow = $time + 3600 * 24 * 7;
				if($nextBill >= $time && $nextBill < $eow)
				{
					if(empty($data[$row->groupKey.$key]))
						$data[$row->groupKey.$key] = $row;
					else
						$data[$row->groupKey.$key]->count += $row->count;
					break;						
				}
			}
		}
		
		$data2 = array();
		
		arrays::reformRows($data,function($row) use (&$data2,&$attritionRates,&$base_weeks,&$weeks_index,&$endDate,&$totals)
		{
			extract((array) $row);
			
			if(empty($data2[$billerId]))
			{
				$data2[$billerId]['weeks'] = $base_weeks;
				$data2[$billerId]['billerName'] = $billerName;
			}
			$start = new DateTime($nextBillDate);
			$end = clone $endDate;		
				
			while($start <= $end)
			{
				$nextBill = $start->getTimestamp();
				
				if(($billingCycleType == 'ONE_TIME' && $billingCycleNumber + 1 > 2))
					break;
					
				if($finalBillingCycle > 0 && $billingCycleNumber + 1 > $finalBillingCycle)
					break;
				
				
				$cur_rate = $billingCycleNumber <=3 ? $attritionRates[$billingCycleNumber] : $attritionRates[4];
				$remaining =  $count * ($cur_rate / 100);
				
				if($billingCycleNumber+1 == 2 && $trialType == 'DELAYED')
					$totalBill -= $shippingPrice;
				
				if($remaining < .5)
					$remaining = 0;
				
				$revenue = ($totalBill) * $remaining;

				foreach($weeks_index as $time=>$key)
				{
					$eow = $time + 3600 * 24 * 7;
					if($nextBill >= $time && $nextBill < $eow)
					{
						$data2[$billerId]['weeks'][$key]['count'] += round($remaining);
						$data2[$billerId]['weeks'][$key]['rev'] += $revenue;
						$data2[$billerId]['weeks'][$key]['cur'] = $currencySymbol;
						
						$totals[$key]['count'] += round($remaining);
						$totals[$key]['rev'] += $revenue;
						$totals[$key]['cur'] = $currencySymbol;
						
						$billingCycleNumber++;
						$count = $remaining;
						break;						
					}
				}
				
				if(empty($billingIntervalDays))
					break;
				
				$start->modify("+{$billingIntervalDays} day");			
			}
		});
		
		$data2 = arrays::sortByKey($data2,'billerName');
		
		
		$element = array('billerName'=>"Total",
						  'weeks'=>$totals,
						  'isTotal'=>true);
								  
		$data2[] = $element;
		
		$result->data = $data2;
		$result->totalResults = count($data2);
		$result->base_weeks = $base_weeks;
		return $result;
	}
	
	function getTable($args=NULL)
	{
		$result = $this->getResult($args);
		$data = $result->data;
		$base_weeks = $result->base_weeks;
		
		$table = '<table class="baseTable">
  <tbody>
	<tr class="titleRow">
		<td style="background:none">&nbsp; </td>
		';
		
		foreach($base_weeks as $week=>$ign)
		{
			$table .= "<td colspan='2' style='background-color:#efefef;text-align:center'> $week </td>\n";	
		}
		$table .= "</tr>
		<tr class='titleRow'>
			<td > Merchant </td>
		";
		foreach($base_weeks as $week=>$ign)
		{
			$table .= "<td> Bills </td><td> Revenue </td>\n";	
		}
		$table .= "</tr>";
		
		
		foreach($data as $row)
		{
			$name = $row['billerName'];
			if($name == 'Total')
				$name = '<b>Total</b>';
			
			$table.= "<tr><td style='white-space:nowrap;word-break:none'>{$name}</td>";
			
			foreach($row['weeks'] as $week=>$stats)
			{
				$rev = !empty($stats['rev']) ? $stats['cur'].number_format($stats['rev'],2) : '---';
				$count = $stats['count'] ?: '---';
				
				if(isset($row['isTotal']))
				{
					$rev = "<b>$rev</b>";
					$count = "<b>$count</b>";
				}
				$table .= "<td>$count</td><td>".$rev."</td>";	
				
			}
			$table .= "</tr>";
			
		}
		
		$table .= "</table>";
		
		
		return $table;
	}
	
	
	function getCsv($args=array())
	{
		
		$result = $this->getResult($args);
		$data = $result->data;	
		
		$file = ',';
		$line2 = 'Mid,';
		foreach($result->base_weeks as $week=>$tmp)
		{
			$file .= $week.",,";
			$line2 .= 'cnt,rev,';
		}
		$line2 = substr($line2,0,-1);
		$file = substr($file,0,-2)."\n";
		$file .= $line2."\n";

		foreach($data as $row)
		{
			$row = (object) $row;
			$name = $row->billerName;
			
			$file .= $name.",";
			foreach($row->weeks as $week=>$stats)
			{
				extract($stats);
				$file .= $count.",";
				$file .= number_format($rev,2,'.','').",";
			}
			$file = substr($file,0,-1)."\n";
		}
		
		$fileInfo = "Projections By Mid\n";
		$fileInfo .= "Date Range: ".$args->startDate.' - '.$args->endDate."\n";
		
		$params = array('campaignId','productId','affiliateId','callCenterId','billerId');
	
		foreach($params as $k)
		{
			if(empty($args->$k))
				continue;
						
			if($k == 'campaignId')
			{
				$fileInfo .= strings::camelToTitle($k).": ".strings::enumToTitle($args->$k)."\n";
				$name = $this->server->fetchValue("SELECT campaignName FROM campaigns WHERE campaignId = ?",$args->$k);
				$fileInfo .= "Campaign Name: ".$name."\n";
			}
			elseif($k == 'productId')
			{
				$fileInfo .= strings::camelToTitle($k).": ".strings::enumToTitle($args->$k)."\n";
				$name = $this->server->fetchValue("SELECT productName FROM products WHERE productId = ?",$args->$k);
				$fileInfo .= "Product Name: ".$name."\n";	
				
			}
			elseif($k == 'affiliateId')
			{
				$fileInfo .= strings::camelToTitle($k).": ".strings::enumToTitle($args->$k)."\n";
				$name = $this->server->fetchValue("SELECT sourceTitle FROM sources WHERE sourceType = 'AFFILIATE' AND sourceId = ?",$args->$k);
				$fileInfo .= "Affiliate Name: ".$name."\n";	
				
			}
			elseif($k == 'callCenterId')
			{
				$fileInfo .= strings::camelToTitle($k).": ".strings::enumToTitle($args->$k)."\n";
				$name = $this->server->fetchValue("SELECT sourceTitle FROM sources WHERE sourceType = 'CALLCENTER' AND sourceId = ?",$args->$k);
				$fileInfo .= "Call Center Name: ".$name."\n";	
			}
			elseif($k == 'billerId')
			{
				$fileInfo .= strings::camelToTitle($k).": ".strings::enumToTitle($args->$k)."\n";
				$name = $this->server->fetchValue("SELECT title FROM billers WHERE billerId = ?",$args->$k);
				$fileInfo .= "MID Name: ".$name."\n";	
			}
			else
			{
				$fileInfo .= strings::camelToTitle($k).": ".strings::enumToTitle($args->$k)."\n";	
			}
		}
		
		return $fileInfo."\n".$file;
	}
	
}
