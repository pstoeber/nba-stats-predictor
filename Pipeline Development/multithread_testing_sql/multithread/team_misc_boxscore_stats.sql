insert into nba_stats_backup.team_misc_boxscore_stats select `b_map`.`game_hash`  AS `game_hash`,
                                                    `a`.`TEAM`           AS `team`,
                                                    `a`.`W/L`            AS `W/L`,
                                                    `a`.`MIN`            AS `min`,
                                                    `a`.`PTS_OFF_TO`     AS `pts_off_to`,
                                                    `a`.`2ND_PTS`        AS `2nd_pts`,
                                                    `a`.`FBPS`           AS `fbps`,
                                                    `a`.`PITP`           AS `PITP`,
                                                    `a`.`OPP_PTS_OFF_TO` AS `opp_pts_off_to`,
                                                    `a`.`OPP_2ND_PTS`    AS `opp_2nd_pts`,
                                                    `a`.`OPP_FBPS`       AS `opp_fbps`,
                                                    `a`.`OPP_PITP`       AS `opp_pitp`
                                             from (`nba_stats`.`box_score_map_view` `b_map`
                                                    join `nba_stats`.`team_misc_boxscore_stats` `a`
                                                         on (((`b_map`.`home_team` = `a`.`TEAM`) and
                                                              (`b_map`.`game_date` = str_to_date(`a`.`GAME_DATE`, '%Y-%m-%d')))))
                                             union
                                             select `b_map`.`game_hash`  AS `game_hash`,
                                                    `a`.`TEAM`           AS `team`,
                                                    `a`.`W/L`            AS `W/L`,
                                                    `a`.`MIN`            AS `min`,
                                                    `a`.`PTS_OFF_TO`     AS `pts_off_to`,
                                                    `a`.`2ND_PTS`        AS `2nd_pts`,
                                                    `a`.`FBPS`           AS `fbps`,
                                                    `a`.`PITP`           AS `PITP`,
                                                    `a`.`OPP_PTS_OFF_TO` AS `opp_pts_off_to`,
                                                    `a`.`OPP_2ND_PTS`    AS `opp_2nd_pts`,
                                                    `a`.`OPP_FBPS`       AS `opp_fbps`,
                                                    `a`.`OPP_PITP`       AS `opp_pitp`
                                             from (`nba_stats`.`box_score_map_view` `b_map`
                                                    join `nba_stats`.`team_misc_boxscore_stats` `a`
                                                         on (((`b_map`.`away_team` = `a`.`TEAM`) and
                                                              (`b_map`.`game_date` = str_to_date(`a`.`GAME_DATE`, '%Y-%m-%d')))));
