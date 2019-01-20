insert into nba_stats_backup.team_scoring_boxscore_stats select `b_map`.`game_hash` AS `game_hash`,
                                                       `a`.`TEAM`          AS `team`,
                                                       `a`.`W/L`           AS `W/L`,
                                                       `a`.`MIN`           AS `min`,
                                                       `a`.`%FGA_2PT`      AS `%fga_2pt`,
                                                       `a`.`%FGA_3PT`      AS `%FGA_3PT`,
                                                       `a`.`%PTS_2PT`      AS `%PTS_2PT`,
                                                       `a`.`%PTS_2PT_MR`   AS `%PTS_2PT_MR`,
                                                       `a`.`%PTS_3PT`      AS `%PTS_3PT`,
                                                       `a`.`%PTS_FBPS`     AS `%PTS_FBPS`,
                                                       `a`.`%PTS_FT`       AS `%PTS_FT`,
                                                       `a`.`%PTS_OFF_TO`   AS `%PTS_OFF_TO`,
                                                       `a`.`%PTS_PITP`     AS `%PTS_PITP`,
                                                       `a`.`2FGM_%AST`     AS `2FGM_%AST`,
                                                       `a`.`2FGM_%UAST`    AS `2FGM_%UAST`,
                                                       `a`.`3FGM_%AST`     AS `3FGM_%AST`,
                                                       `a`.`3FGM_%UAST`    AS `3FGM_%UAST`,
                                                       `a`.`FGM_%AST`      AS `FGM_%AST`,
                                                       `a`.`FGM_%UAST`     AS `FGM_%UAST`
                                                from (`nba_stats`.`box_score_map_view` `b_map`
                                                       join `nba_stats`.`team_scoring_boxscore_stats` `a`
                                                            on (((`b_map`.`home_team` = `a`.`TEAM`) and
                                                                 (`b_map`.`game_date` = str_to_date(`a`.`GAME_DATE`, '%Y-%m-%d')))))
                                                union
                                                select `b_map`.`game_hash` AS `game_hash`,
                                                       `a`.`TEAM`          AS `team`,
                                                       `a`.`W/L`           AS `W/L`,
                                                       `a`.`MIN`           AS `min`,
                                                       `a`.`%FGA_2PT`      AS `%fga_2pt`,
                                                       `a`.`%FGA_3PT`      AS `%FGA_3PT`,
                                                       `a`.`%PTS_2PT`      AS `%PTS_2PT`,
                                                       `a`.`%PTS_2PT_MR`   AS `%PTS_2PT_MR`,
                                                       `a`.`%PTS_3PT`      AS `%PTS_3PT`,
                                                       `a`.`%PTS_FBPS`     AS `%PTS_FBPS`,
                                                       `a`.`%PTS_FT`       AS `%PTS_FT`,
                                                       `a`.`%PTS_OFF_TO`   AS `%PTS_OFF_TO`,
                                                       `a`.`%PTS_PITP`     AS `%PTS_PITP`,
                                                       `a`.`2FGM_%AST`     AS `2FGM_%AST`,
                                                       `a`.`2FGM_%UAST`    AS `2FGM_%UAST`,
                                                       `a`.`3FGM_%AST`     AS `3FGM_%AST`,
                                                       `a`.`3FGM_%UAST`    AS `3FGM_%UAST`,
                                                       `a`.`FGM_%AST`      AS `FGM_%AST`,
                                                       `a`.`FGM_%UAST`     AS `FGM_%UAST`
                                                from (`nba_stats`.`box_score_map_view` `b_map`
                                                       join `nba_stats`.`team_scoring_boxscore_stats` `a`
                                                            on (((`b_map`.`away_team` = `a`.`TEAM`) and
                                                                 (`b_map`.`game_date` = str_to_date(`a`.`GAME_DATE`, '%Y-%m-%d')))));
