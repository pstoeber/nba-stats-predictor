insert into nba_stats_backup.player_scoring_stats (select `b`.`game_hash`  AS `game_hash`,
                                                 `pv`.`player_id` AS `player_id`,
                                                 `s`.`%FGA2PT`    AS `%FGA2PT`,
                                                 `s`.`%FGA3PT`    AS `%fga3pt`,
                                                 `s`.`%PTS2PT`    AS `%PTS2PT`,
                                                 `s`.`%PTS2PT MR` AS `%PTS2PT MR`,
                                                 `s`.`%PTS3PT`    AS `%PTS3PT`,
                                                 `s`.`%PTSFBPs`   AS `%ptsfbps`,
                                                 `s`.`%PTSFT`     AS `%ptsft`,
                                                 `s`.`%PTSOffTO`  AS `%PTSOffTO`,
                                                 `s`.`%PTSPITP`   AS `%PTSPITP`,
                                                 `s`.`2FGM%AST`   AS `2fgm%ast`,
                                                 `s`.`2FGM%UAST`  AS `2fgm%uast`,
                                                 `s`.`3FGM%AST`   AS `3fgm%ast`,
                                                 `s`.`3FGM%UAST`  AS `3fgm%uast`,
                                                 `s`.`FGM%AST`    AS `fgm%ast`,
                                                 `s`.`FGM%UAST`   AS `fgm%uast`
                                          from (((((select `box_score_map_view`.`game_hash` AS `game_hash`,
                                                           `box_score_map_view`.`home_team` AS `team`,
                                                           `box_score_map_view`.`game_date` AS `game_date`
                                                    from `nba_stats`.`box_score_map_view`
                                                    union
                                                    select `box_score_map_view`.`game_hash` AS `game_hash`,
                                                           `box_score_map_view`.`away_team` AS `team`,
                                                           `box_score_map_view`.`game_date` AS `game_date`
                                                    from `nba_stats`.`box_score_map_view`) `b` join `nba_stats`.`game_date_lookup` `lu` on ((`b`.`game_date` = `lu`.`day`))) join `nba_stats`.`player_team_map_view` `pv` on (((`lu`.`season` = `pv`.`season`) and (`b`.`team` = `pv`.`team`)))) join `nba_stats`.`player_info` `p` on ((`pv`.`player_id` = `p`.`player_id`)))
                                                 join `nba_stats`.`player_scoring_stats` `s`
                                                      on (((`b`.`team` = `s`.`team`) and (`p`.`NAME` = `s`.`name`) and
                                                           (`b`.`game_date` = `s`.`game_date`)))));
