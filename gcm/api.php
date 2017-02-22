<?php
	require_once("Rest.inc.php");
	
	class API extends REST {
	
		public $data = "";
		
		
		public function __construct(){
			parent::__construct();				// Init parent contructor
		}
		/*
		 *  Database connection 
		*/
		public function generatePinCode(){
			return rand(1000,10000);
		}
		
		public function processApi(){
			$json = array();
			$func = $_REQUEST['cmd'];
			
			if( $func == 'register' ){
				$json_data1 	= $_POST['json_data'];
				$json_data 		= str_replace("\\", "", $json_data1);
				$regData	 	= json_decode($json_data);
				
				$name 		= $regData->{'name'};
				$email 		= $regData->{'email'};
				$gcm_regid		= $regData->{'regid'};
				
				include_once './db_functions.php';
				include_once './GCM.php';

				$db = new DB_Functions();
				$gcm = new GCM();

				$res = $db->storeUser($name, $email, $gcm_regid);

				$registatoin_ids = array($gcm_regid);
				$message = array("product" => "shirt");

				$result = $gcm->send_notification($registatoin_ids, $message);
				echo($result);
				return;
			}	
			if( $func == 'receive' ){
				$json_data1 	= $_POST['json_data'];
				$json_data 		= str_replace("\\", "", $json_data1);
				$regData	 	= json_decode($json_data);
				
				$sender_regId 		= $regData->{'sender_regId'};
				$receiver_regId 		= $regData->{'receiver_regId'};
				$msg		= $regData->{'msg'};
				
				include_once './db_functions.php';
				include_once './GCM.php';

				$db = new DB_Functions();
				$gcm = new GCM();

				$res = $db->saveMessage($sender_regId, $receiver_regId, $msg);

				$registatoin_ids = array($sender_regId);
				$message = "Successfully receive.";

				$result = $gcm->send_notification($registatoin_ids, $message);
				echo($result);
				return;
			}	
		}
		private function json($data){
			if(is_array($data)){
				return json_encode($data);
			}
		}
	}
	$api = new API;
	$api->processApi();
?>