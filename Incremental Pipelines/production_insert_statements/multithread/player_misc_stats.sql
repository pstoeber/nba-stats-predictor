insert into nba_stats_prod.player_misc_stats (select `b`.`game_hash`        AS `game_hash`,
                                              `pv`.`player_id`       AS `player_id`,
                                              `misc`.`PTS_OFF_TO`    AS `pts_off_to`,
                                              `misc`.`2nd_PTS`       AS `2nd_pts`,
                                              `misc`.`FBPs`          AS `fbps`,
                                              `misc`.`PITP`          AS `pitp`,
                                              `misc`.`OppPTS_OFF_TO` AS `opppts_off_to`,
                                              `misc`.`Opp2nd_PTS`    AS `opp2nd_pts`,
                                              `misc`.`OppFBPs`       AS `oppfbps`,
                                              `misc`.`OppPITP`       AS `opppitp`,
                                              `misc`.`BLK`           AS `blk`,
                                              `misc`.`BLKA`          AS `blka`,
                                              `misc`.`PF`            AS `pf`,
                                              `misc`.`PFD`           AS `pfd`
                                       from (((((select `box_score_map_view`.`game_hash` AS `game_hash`,
                                                        `box_score_map_view`.`home_team` AS `team`,
                                                        `box_score_map_view`.`game_date` AS `game_date`
                                                 from `nba_stats`.`box_score_map_view`
                                                 union
                                                 select `box_score_map_view`.`game_hash` AS `game_hash`,
                                                        `box_score_map_view`.`away_team` AS `team`,
                                                        `box_score_map_view`.`game_date` AS `game_date`
                                                 from `nba_stats`.`box_score_map_view`) `b` join `nba_stats`.`game_date_lookup` `lu` on ((`b`.`game_date` = `lu`.`day`))) join `nba_stats`.`player_team_map_view` `pv` on (((`lu`.`season` = `pv`.`season`) and (`b`.`team` = `pv`.`team`)))) join `nba_stats`.`player_info` `p` on ((`pv`.`player_id` = `p`.`player_id`)))
                                              join `nba_stats`.`player_misc_stats` `misc`
                                                   on (((`b`.`team` = `misc`.`team`) and
                                                        (`p`.`NAME` = `misc`.`name`) and
                                                        (`b`.`game_date` = `misc`.`game_date`)))));
