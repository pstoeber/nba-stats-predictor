insert into nba_stats_backup.active_rosters (select distinct `p`.`PLAYER_ID`  AS `player_id`,
                                                    `a`.`name`       AS `name`,
                                                    `a`.`team_id`    AS `team_id`,
                                                    `a`.`team`       AS `team`,
                                                    `a`.`conference` AS `conference`
                                    from (`nba_stats`.`active_rosters` `a`
                                           left join `nba_stats`.`player_info` `p` on ((`p`.`NAME` = `a`.`name`))));
