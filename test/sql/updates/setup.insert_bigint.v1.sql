-- Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
--
-- This file is licensed under the Apache License,
-- see LICENSE-APACHE at the top level directory.


INSERT INTO public."two_Partitions"("timeCustom", device_id, series_0, series_1, series_2) VALUES
(1257987600000000000, 'dev1', 1.5, 1, 1),
(1257987600000000000, 'dev1', 1.5, 2, 2),
(1257894000000000000, 'dev2', 1.5, 1, 3),
(1257894002000000000, 'dev1', 2.5, 3, 4);

INSERT INTO "two_Partitions"("timeCustom", device_id, series_0, series_1, series_2) VALUES
(1257894000000000000, 'dev2', 1.5, 2, 6);
