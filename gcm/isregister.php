<?php

	$json = array();

    	$imei		= $_REQUEST["data1"];
    	$meid     	= $_REQUEST["data2"];
	$simserial	= $_REQUEST["data3"];
	$androidid	= $_REQUEST["data4"];
	
	include_once './db_functions.php';

   	$db = new DB_Functions();

    	$res = $db->isDeviceExisted($imei, $meid, $simserial, $androidid);
	//$message = array("register" => $res);
    	
    	echo $res;
    
?>