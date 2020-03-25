package inha.nsl.easytrack.library

import android.content.Context
import android.database.Cursor
import android.database.sqlite.SQLiteDatabase
import android.util.Log
import inha.nsl.easytrack.library.Tools.TAG

object LocalDBManager {
    private lateinit var db: SQLiteDatabase
    internal var notInitialized = true

    fun init(context: Context) {
        db = context.openOrCreateDatabase(context.packageName, Context.MODE_PRIVATE, null)
        db.execSQL("create table if not exists Data(id integer primary key autoincrement, dataSourceId int default(0), timestamp bigint default(0), accuracy float default(0.0), data varchar(512) default(null));")
        db.execSQL("create table if not exists AppUse(id integer primary key autoincrement, package_name varchar(256), start_timestamp bigint, end_timestamp bigint, total_time_in_foreground bigint);")
        notInitialized = false
    }

    fun saveNumericData(sensorId: Int, timestamp: Long, accuracy: Float, data: FloatArray) {
        if (notInitialized) {
            Log.e(TAG, "LocalDbManager: please make sure to call LocalDbManager.init() before making any other function calls!")
            return
        }
        val sb = StringBuilder()
        for (value in data) sb.append(value).append(',')
        if (sb.isNotEmpty()) sb.replace(sb.length - 1, sb.length, "")
        saveStringData(sensorId, timestamp, accuracy, sb.toString())
    }

    fun saveMixedData(sensorId: Int, timestamp: Long, accuracy: Float, vararg params: Any) {
        if (notInitialized) {
            Log.e(TAG, "LocalDbManager: please make sure to call LocalDbManager.init() before making any other function calls!")
            return
        }
        val sb = StringBuilder()
        for (value in params) sb.append(value).append(',')
        if (sb.isNotEmpty()) sb.replace(sb.length - 1, sb.length, "")
        saveStringData(sensorId, timestamp, accuracy, sb.toString())
    }

    private fun saveStringData(dataSourceId: Int, timestamp: Long, accuracy: Float, data: String) {
        if (notInitialized) {
            Log.e(TAG, "LocalDbManager: please make sure to call LocalDbManager.init() before making any other function calls!")
            return
        }
        db.execSQL(
            "insert into Data(dataSourceId, timestamp, accuracy, data) values(?, ?, ?, ?);",
            arrayOf(
                dataSourceId,
                timestamp,
                accuracy,
                data
            )
        )
    }

    private fun getOverlappingAppUseRecords(packageName: String, startTimestamp: Long, endTimestamp: Long): List<AppUsageRecord>? {
        if (notInitialized) {
            Log.e(TAG, "LocalDbManager: please make sure to call LocalDbManager.init() before making any other function calls!")
            return null
        }
        val res = mutableListOf<AppUsageRecord>()
        val cursor = db.rawQuery(
            "select * from AppUse where package_name=? and (start_timestamp=$startTimestamp or end_timestamp=$endTimestamp or (start_timestamp < $startTimestamp and $startTimestamp < end_timestamp) or (start_timestamp < $endTimestamp and $endTimestamp < end_timestamp) or (start_timestamp < $startTimestamp and $endTimestamp < end_timestamp))",
            arrayOf(
                packageName
            )
        )
        if (cursor.moveToFirst())
            do {
                res.add(
                    AppUsageRecord(
                        id = cursor.getInt(0),
                        packageName = cursor.getString(1),
                        startTimestamp = cursor.getLong(2),
                        endTimestamp = cursor.getLong(3),
                        totalTimeInForeground = cursor.getLong(4)
                    )
                )
            } while (cursor.moveToNext())
        cursor.close()
        return res
    }

    private fun getLastAppUseRecord(packageName: String): AppUsageRecord? {
        if (notInitialized) {
            Log.e(TAG, "LocalDbManager: please make sure to call LocalDbManager.init() before making any other function calls!")
            return null
        }

        val cursor = db.rawQuery(
            "select * from AppUse where package_name=? order by end_timestamp desc limit(1);",
            arrayOf(packageName)
        )

        return if (cursor.moveToFirst()) {
            val res = AppUsageRecord(
                id = cursor.getInt(0),
                packageName = cursor.getString(1),
                startTimestamp = cursor.getLong(2),
                endTimestamp = cursor.getLong(3),
                totalTimeInForeground = cursor.getLong(4)
            )
            cursor.close()
            res
        } else {
            cursor.close()
            null
        }
    }

    private fun isUniqueAppUseRecord(packageName: String, startTimestamp: Long): Boolean? {
        if (notInitialized) {
            Log.e(TAG, "LocalDbManager: please make sure to call LocalDbManager.init() before making any other function calls!")
            return null
        }

        val cursor = db.rawQuery(
            "select exists(select 1 from AppUse where package_name=? and start_timestamp=$startTimestamp)",
            arrayOf(packageName)
        )
        val res = cursor.moveToFirst() && cursor.getInt(0) <= 0
        cursor.close()
        return res
    }

    fun saveAppUsageStat(packageName: String, endTimestamp: Long, totalTimeInForeground: Long) {
        if (notInitialized) {
            Log.e(TAG, "LocalDbManager: please make sure to call LocalDbManager.init() before making any other function calls!")
            return
        }

        val lastRecord = getLastAppUseRecord(packageName)
        if (lastRecord == null)
            db.execSQL(
                "insert into AppUse(package_name, start_timestamp, end_timestamp, total_time_in_foreground) values(?, ?, ?, ?);",
                arrayOf(
                    packageName,
                    endTimestamp - totalTimeInForeground,
                    endTimestamp,
                    totalTimeInForeground
                )
            )
        else {
            val startTimestamp =
                endTimestamp - (totalTimeInForeground - lastRecord.totalTimeInForeground)
            if (startTimestamp != endTimestamp) {
                if (startTimestamp == lastRecord.endTimestamp)
                    db.execSQL(
                        "update AppUse set end_timestamp = ? and total_time_in_foreground = ? where id=?;",
                        arrayOf(
                            endTimestamp,
                            totalTimeInForeground,
                            lastRecord.id
                        )
                    )
                else if (isUniqueAppUseRecord(packageName, startTimestamp)!!) {
                    val overlappingElements = getOverlappingAppUseRecords(packageName, startTimestamp, endTimestamp)
                    if (overlappingElements!!.isEmpty())
                        db.execSQL(
                            "insert into AppUse(package_name, start_timestamp, end_timestamp, total_time_in_foreground) values(?, ?, ?, ?);",
                            arrayOf(
                                packageName,
                                startTimestamp,
                                endTimestamp,
                                totalTimeInForeground
                            )
                        )
                    else {
                        var minStartTimestamp = startTimestamp
                        var maxEndTimestamp = endTimestamp
                        var maxTotalTimeInForeground = totalTimeInForeground
                        for (appUse in overlappingElements) {
                            if (appUse.startTimestamp < minStartTimestamp)
                                minStartTimestamp = appUse.startTimestamp
                            if (appUse.endTimestamp > maxEndTimestamp)
                                maxEndTimestamp = appUse.endTimestamp
                            if (appUse.totalTimeInForeground > maxTotalTimeInForeground)
                                maxTotalTimeInForeground = appUse.totalTimeInForeground
                            db.execSQL("delete from AppUse where id=${appUse.id};")
                        }
                        db.execSQL(
                            "insert into AppUse(package_name, start_timestamp, end_timestamp, total_time_in_foreground) values(?,$minStartTimestamp,$maxEndTimestamp,$maxTotalTimeInForeground);",
                            arrayOf(packageName)
                        )
                    }
                }
            }
        }
    }

    @Synchronized
    fun cleanDb() {
        if (notInitialized) {
            Log.e(TAG, "LocalDbManager: please make sure to call LocalDbManager.init() before making any other function calls!")
            return
        }
        db.execSQL("delete from Data;")
    }

    private fun countSamples(tableName: String): Int {
        val cursor = db.rawQuery("select count(*) from $tableName;", arrayOfNulls(0))
        var result = 0
        if (cursor.moveToFirst()) result = cursor.getInt(0)
        cursor.close()
        return result
    }

    fun countSamples(): Int? {
        if (notInitialized) {
            Log.e(TAG, "LocalDbManager: please make sure to call LocalDbManager.init() before making any other function calls!")
            return null
        }
        return countSamples("AppUse") + countSamples("Data")
    }

    fun sensorData(): Cursor? {
        if (notInitialized) {
            Log.e(TAG, "LocalDbManager: please make sure to call LocalDbManager.init() before making any other function calls!")
            return null
        }
        return db.rawQuery("select * from Data;", arrayOfNulls(0))
    }

    fun deleteRecord(id: Int) {
        if (notInitialized) {
            Log.e(TAG, "LocalDbManager: please make sure to call LocalDbManager.init() before making any other function calls!")
            return
        }
        db.execSQL("delete from Data where id=?;", arrayOf<Any>(id))
    }

    private class AppUsageRecord(
        var id: Int,
        var packageName: String,
        var startTimestamp: Long,
        var endTimestamp: Long,
        var totalTimeInForeground: Long
    )
}
