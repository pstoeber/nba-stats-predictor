insert into nba_stats_backup.regularseasonmisctotals (select distinct `nba_stats`.`regularseasonmisctotals`.`PLAYER_ID` AS `player_id`,
                                                       `nba_stats`.`regularseasonmisctotals`.`SEASON`    AS `season`,
                                                       `nba_stats`.`regularseasonmisctotals`.`TEAM`      AS `team`,
                                                       `nba_stats`.`regularseasonmisctotals`.`DBLDBL`    AS `dbldbl`,
                                                       `nba_stats`.`regularseasonmisctotals`.`TRIDBL`    AS `tridbl`,
                                                       `nba_stats`.`regularseasonmisctotals`.`DQ`        AS `dq`,
                                                       `nba_stats`.`regularseasonmisctotals`.`EJECT`     AS `eject`,
                                                       `nba_stats`.`regularseasonmisctotals`.`TECH`      AS `tech`,
                                                       `nba_stats`.`regularseasonmisctotals`.`FLAG`      AS `flag`,
                                                       `nba_stats`.`regularseasonmisctotals`.`AST/TO`    AS `ast/to`,
                                                       `nba_stats`.`regularseasonmisctotals`.`STL/TO`    AS `stl/to`,
                                                       `nba_stats`.`regularseasonmisctotals`.`RAT`       AS `rat`,
                                                       `nba_stats`.`regularseasonmisctotals`.`SCEFF`     AS `sceff`,
                                                       `nba_stats`.`regularseasonmisctotals`.`SHEFF`     AS `sheff`
                                       from `nba_stats`.`regularseasonmisctotals`
                                       where ((`nba_stats`.`regularseasonmisctotals`.`TEAM` <> '--') and
                                              `nba_stats`.`regularseasonmisctotals`.`PLAYER_ID` in
                                              (select `player_info_view`.`player_id`
                                               from `nba_stats`.`player_info_view`)));
