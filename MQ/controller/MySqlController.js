var mySqlConnect = function () {
  this.mysql = require('mysql');
//mysqldump -h database-1.cpkbtqcxegai.us-east-2.rds.amazonaws.com -u masteraha   > _2019.sql
  this.connection = this.mysql.createConnection({
    host     : 'masterretailt1.cmv7b97kioir.us-east-1.rds.amazonaws.com',
    user     : 'master_ret_t1',
    password : 'r3tt0hsdos.',
    port     : '3306',
    database : 'masterdb'
})
};

module.exports = mySqlConnect ;

mySqlConnect.prototype.connect = function () {
	var me = this;

   // used to deserialize the user

	  me.connection.connect(function(err) {
	  if (!err)
	  console.log('Mysql is already... Y listo!')
      if(err)
	  console.log('Error al conectar sql; ')
	})

};



//save msg

mySqlConnect.prototype.saveMessage = function(msg,callback){
	var me = this
    var up_today = me.moment().format('YYYY-MM-DD HH:mm:ss');
		  me.connection.query('INSERT INTO message (id,autor,message) VALUES(NULL,"'+msg.autor+'","'+msg.message+'");', function(err, results) {
    			if (!err){
        			callback(results);
              console.log(results)
          }
          if(err){
              console.log(err);
              callback({err:true});
          }
		  })
};
