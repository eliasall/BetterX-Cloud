<?php

    $json = array();

    $sender_regId 	= $_REQUEST["data1"];
    $receiver_regId     = $_REQUEST["data2"];
    $msg 		= $_REQUEST["data3"];
	
    include_once './db_functions.php';
    include_once './GCM.php';

    $db = new DB_Functions();
    $gcm = new GCM();

    if( $msg != "" ){
       
       $registatoin_ids = array($sender_regId);
       $message = array("price" => $msg);
       $result = $gcm->send_notification($registatoin_ids, $message );
       
       $jsonArray = json_decode($result);

	if(!empty($jsonArray->results))
	{
		$results = $jsonArray->results[0];
		if(isset($results->message_id) )
	    	{
			$res = $db->saveMessage($sender_regId, $receiver_regId, $results->message_id, $msg);
		}
	}
	echo $msg;
    }
?>