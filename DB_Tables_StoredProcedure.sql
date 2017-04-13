CREATE TABLE linklion2.dataset2 (
  indDS int(11) NOT NULL AUTO_INCREMENT,
  name varchar(200) DEFAULT NULL,
  PRIMARY KEY (indDS)
);

CREATE TABLE linklion2.uri2 (
  indURI int(11) NOT NULL AUTO_INCREMENT,
  uri varchar(900) DEFAULT NULL,
  indexDataset int(11) DEFAULT NULL,
  countDType int(11) DEFAULT NULL,
  PRIMARY KEY (indURI)
  );

DELIMITER //
CREATE DEFINER=`root`@`localhost` PROCEDURE `ADD_DB`(IN _subj varchar(900), _datasetEndPoint varchar(900))
BEGIN
	DECLARE indexDS int(11) DEFAULT 0;
	DECLARE cDtypes int(11) DEFAULT 0;
    /*Select 'Starting... First Select'; */
	Select indDS INTO indexDS from linklion2.dataset2 where name = _datasetEndPoint;	
	/* select concat('indexDS = ', indexDS); */
    IF (indexDS < 1) Then
	Begin
		/* select concat('Inserting dataset: ', indexDS); */
		insert into linklion2.dataset2(name) values (_datasetEndPoint);	
		COMMIT;
	end;
	End if;
	Select max(indDS) INTO indexDS from linklion2.dataset2;
	Select sum(countDType) into cDtypes from linklion2.uri2 where 
		uri = _subj and indexDataset = indexDS;
	
    if(cDtypes IS NOT NULL) then
    Begin
		/* select (cDtypes + 1) into cDtypes; */
		Update linklion2.uri2 set countDType = (cDtypes + 1) 
        where uri = _subj and indexDataset = indexDS;
	End;
    else
    Begin
		Insert into linklion2.uri2(uri,indexDataset,countDType) values (_subj, indexDS, 1);
    End;
    End if;
    COMMIT;
END
//
