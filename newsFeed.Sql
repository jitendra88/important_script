SELECT NM.*
FROM   news_mappings AS NM
       LEFT JOIN news AS N
              ON N.id = NM.newsid
       LEFT JOIN boards AS BRD
              ON NM.boardid = BRD.id
       INNER JOIN users AS U
               ON ( N.userid = U.id
                    AND U.status = 1
                    AND U.deactivatedbyadmin = 0 )
       LEFT JOIN news_university_mappings NUM
              ON NM.id = NUM.newsmappingid
WHERE  N.status = 1
       AND (( ( N.userid = 92649
               AND ( NM.masterposttypeid = 1
                      OR BRD.isarchived = 0 ) )
              OR ( N.userid IN(SELECT F.followeduserid
                               FROM   follow AS F
                               WHERE  F.followeruserid = 92649)
                   AND NM.masterposttypeid = 1 )
              OR ( BRD.id IN(SELECT boardid
                             FROM   board_join
                             WHERE  requeststatus = 'approved'
                                    AND joinedby = 92649)
                   AND BRD.isarchived = 0 ) )
        OR ( NM.masterposttypeid = 3
             AND NUM.universityid = 65 ) )
        AND NM.id < 96720
        AND NM.createdat <= From_unixtime(1498093849)
GROUP  BY NM.id
ORDER  BY NM.createdat DESC,
          NM.id DESC
LIMIT  10