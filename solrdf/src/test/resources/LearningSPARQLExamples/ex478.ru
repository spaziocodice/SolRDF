# filename: ex478.ru

PREFIX dcterms: <http://purl.org/dc/terms/> 

DELETE {?document dcterms:dateCopyrighted "2013" . }
INSERT {?document dcterms:dateCopyrighted "2014" . }
WHERE  {?document dcterms:dateCopyrighted "2013" . }
