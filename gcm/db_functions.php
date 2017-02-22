<?php

class DB_Functions {

    private $db;

    // constructor
    function __construct() {
        include_once './db_connect.php';
        // connecting to database
        $this->db = new DB_Connect();
        $this->db->connect();
    }

    // destructor
    function __destruct() {
        
    }

    /**
     * Storing new user
     * returns user details
     */
    public function storeUser($imei, $meid, $simserial, $androidid, $gcm_regid) {
        // insert user into database
        $result = mysql_query("INSERT INTO gcm_users(imei, meid, simserial, androidid, gcm_regid, created) VALUES('$imei', '$meid', '$simserial', '$androidid', '$gcm_regid', NOW())");
        // check for successful store
        if ($result) {
            // get user details
            //$id = mysql_insert_id(); // last inserted id
            //$result = mysql_query("SELECT * FROM gcm_devices WHERE id = $id") or die(mysql_error());
            // return user details
            //if (mysql_num_rows($result) > 0) {
            //    return mysql_fetch_array($result);
            //} else {
            //    return false;
            //}
            return true;
        } else {
            return false;
        }
    }
    /*
    public function deleteUser($id) {
        // insert user into database
        $result = mysql_query("Delete From gcm_users where id=".$id);
        if ($result) {
            return "true";
        } else {
            return "false";
        }
    }
    */
    public function deleteUser($regid) {
        // insert user into database
        $result = mysql_query("Delete From gcm_users where gcm_regid='".$regid."'");
        if ($result) {
            return "true";
        } else {
            return "false";
        }
    }
	public function saveMessage($sender_id, $receiver_id, $msg_id, $msg_content) {
        // insert user into database
        $result = mysql_query("INSERT INTO gcm_msgs(sender_id,receiver_id,msg_id, msg_content) VALUES('$sender_id', '$receiver_id', '$msg_id', '$msg_content')");
        // check for successful store
        if ($result) {
            // get user details
            $id = mysql_insert_id(); // last inserted id
            $result = mysql_query("SELECT * FROM gcm_msgs WHERE id = $id") or die(mysql_error());
            // return user details
            if (mysql_num_rows($result) > 0) {
                return mysql_fetch_array($result);
            } else {
                return false;
            }
        } else {
            return false;
        }
    }
	
	public function saveMessageJSON($sender_id, $receiver_id, $msg_id, $msg_content, $json) {
        // insert user into database
        $result = mysql_query("INSERT INTO gcm_msgs(sender_id,receiver_id,msg_id, msg_content,json) VALUES('$sender_id', '$receiver_id', '$msg_id', '$msg_content', '$json')");
        // check for successful store
        if ($result) {
            // get user details
            $id = mysql_insert_id(); // last inserted id
            $result = mysql_query("SELECT * FROM gcm_msgs WHERE id = $id") or die(mysql_error());
            // return user details
            if (mysql_num_rows($result) > 0) {
                return mysql_fetch_array($result);
            } else {
                return false;
            }
        } else {
            return false;
        }
    }
	
	
    /**
     * Get user by email and password
     */
    public function getUserByEmail($email) {
        $result = mysql_query("SELECT * FROM gcm_users WHERE email = '$email' LIMIT 1");
        return $result;
    }

    /**
     * Getting all users
     */
     
    public function getAllUsers() {
        $result = mysql_query("select * FROM gcm_users");
        return $result;
    }
    public function getAllMessages() {
        $result = mysql_query("select * FROM gcm_msgs");
        return $result;
    }
	
	public function getAllFiles() {
        $result = mysql_query("SELECT * FROM file_log order by id desc");
        return $result;
    }

    /**
     * Check user is existed or not
     */
    public function isUserExisted($email) {
        $result = mysql_query("SELECT email from gcm_users WHERE email = '$email'");
        $no_of_rows = mysql_num_rows($result);
        if ($no_of_rows > 0) {
            // user existed
            return true;
        } else {
            // user not existed
            return false;
        }
    }
    public function isDeviceExisted($imei, $meid, $simserial, $androidid) {
        $result = mysql_query("SELECT * from gcm_users WHERE imei = '$imei'");
        $no_of_rows = mysql_num_rows($result);
        if ($no_of_rows > 0) {
            // user existed
            return "true";
        } else {
            // user not existed
            return "false";
        }
    }

}

?>