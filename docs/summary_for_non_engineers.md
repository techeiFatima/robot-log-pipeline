# What the robot logs show: a one-page summary

**The question:** how often does the robot stop itself, when, and under
what conditions? And can we be told quickly when it happens?

**The data:** one day (26 October 2022) of logs from a UR3 collaborative
robot arm, from a public research dataset (UR3 CobotOps, UCI Machine
Learning Repository). It stands in for a real company's robot logs. This
project is not affiliated with any company. The robot wrote one line
about every second: joint temperatures, motor currents and speeds, and
two yes/no flags, "protective stop" and "grip lost".

## What we found

1. **The robot stopped itself 108 times.** A *protective stop* is the
   robot halting on its own for safety. Each stop was short: 1.6 seconds
   on average, 11 seconds at most.
2. **That is about 52 stops per hour of recording.** The day spans 7.3
   hours, but the robot was only recorded for about 2.1 of them. There is
   one long break of 5.2 hours (roughly 09:06 to 14:19 UTC) and two short
   ones.
3. **Stops became more frequent as the day went on.** The share of log
   lines marked "stopped" rose from 2.1 % between 08:00 and 09:00 to
   5.6 % between 15:00 and 16:00 (all times UTC). Of the 108 stops, 21 were in the morning
   recording and 87 in the afternoon.
4. **Stops go with a warmer robot.** The average joint temperature rose
   from 35.2 °C (08:00 hour) to 41.8 °C (15:00 hour). Grouping the 240
   work cycles by temperature: 10 % of the coolest third had a stop,
   against 61 % of the hottest third. **This is a link, not proof of a
   cause.** Temperature and time of day rise together, so this data
   cannot separate them.
5. **Losing grip is rarely part of a stop.** The gripper lost its grip in
   243 log lines, but only 1 of the 108 stops included a grip loss.
6. **78 of 240 work cycles had at least one stop.**

## How much can we trust it

- **Nothing was dropped or guessed.** 54 log lines (0.7 %) have no
  stop/no-stop value and almost no sensor readings. They are kept,
  labelled "unknown" and counted separately. A check on every run proves
  that every stop in the source file is a stop in the cleaned data (278
  of 278 stop lines), and that the 54 unknown lines are still there.
- **Some details of the dataset are not documented,** for example what
  negative current or speed values mean, and whether "Temperature_T0" is
  joint 0. We kept the values as delivered and wrote these down as open
  questions.
- **One day, one robot.** The patterns above describe this recording.
  Months of data from more robots would be needed before acting on them.

## What was built

- A pipeline that loads, cleans and checks the logs every day, and
  **alerts** if data stops arriving, volumes drop, or a check fails.
- A **live alert within 5 minutes of a protective stop.** In a test that
  replayed the recording as if it were live, alerts arrived a median of
  0.016 seconds after the robot "sent" the stop, and never more than 2.2
  seconds. (The replay is a simulation; no real robot was connected.)
- A first **prediction model** for upcoming stops. Its honest results are
  in the README.
