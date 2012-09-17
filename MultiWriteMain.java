package magiclog.useQilunWay.multiWrite;

import java.sql.Connection;  
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import magiclog.ConnHelper;
import magiclog.GmConfig;
import magiclog.LikeRedisBean;
import magiclog.LikeRedisList;
import magiclog.ParseJsonException;
//import magiclog.event.UpdateForMdRollback;
import magiclog.jdbc.JdbcTemplateConnectorHelper;
import magiclog.reconstruction.ReconstructionHelper;
//import magiclog.sqlite.RollbackBean;
import magiclog.useQilunWay.ConfigHelper;
import magiclog.useQilunWay.controlCpu.MyThreadsGroup;
import magiclog.usrRedisWrite.eventHelper.DealJson2OracleTable;
import magiclog.usrRedisWrite.eventHelper.HelperFactory;
import magiclog.usrRedisWrite.eventHelper.gsonArray2Object.GsonHelper;

public class MultiWriteMain {
	private static Logger logger = Logger.getLogger("multiWrite");

	public static void main(String args[]) throws SQLException {
		/**
		 * 
		 */
		if(args != null){
			//tableName
			ConfigHelper.TBALE_NAME = args[0];
			//开始时间
			ConfigHelper.FROM_DATE = args[1];
			//结束时间
			ConfigHelper.TO_DATE = args[2];
			//当前区服
			GmConfig.CURR_QUFU = args[3];
		}
		
		System.out.println("读的表名：" + ConfigHelper.TBALE_NAME);
		System.out.println("开始时间：" + ConfigHelper.FROM_DATE);
		System.out.println("结束时间：" + ConfigHelper.TO_DATE);
		
		// mySql
		DataSource dataSource = JdbcTemplateConnectorHelper.dataSource;
		Connection conn = dataSource.getConnection();
		// 分配循环次数
		int fromId = getMinId(conn);
		int toId = getMaxId(conn);
		System.out.println(fromId + "   " + toId);

		// 一些准备工作
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		// 计算需要写的线程数
		int writeThreadsNum = 0;
		for (int i = 0; i < ReconstructionHelper.eventName.length; i++) {
			if (ReconstructionHelper.IS_DEAL[i] == 1) {
				writeThreadsNum++;
			}
		}
		HelperFactory factory = new HelperFactory();
		MultiWriteWay multiWriteWay = new MultiWriteWay(writeThreadsNum);

		while (fromId < toId) {
			// 读操作，从MySql读数据写入缓冲区中============================================
			if (fromId + ConfigHelper.BLOCK_SIZE < toId) {
				pstmt = conn.prepareStatement("select * from "
						+ ConfigHelper.TBALE_NAME + " where id >= " + fromId
						+ " and id < " + (fromId + ConfigHelper.BLOCK_SIZE));
			} else {
				pstmt = conn.prepareStatement("select * from "
						+ ConfigHelper.TBALE_NAME + " where id >= " + fromId
						+ " and id < " + toId);
			}
			rs = pstmt.executeQuery();
			List<LikeRedisBean> likeRedisList = new ArrayList<LikeRedisBean>();
//			List<RollbackBean> rollbackList = new ArrayList<RollbackBean>();
			while (rs.next()) {
				// 这一部分数据读取一般认为其是很安全的，难以出错的
				int idNo = rs.getInt("id");
//				int isDeal = rs.getInt("isdeal");
				// String logDate = ECDateTool.df.format(rs
				// .getTimestamp("log_date"));
				String logMessage = rs.getString("log_msg");
				String qufu = rs.getString("host");
				if (logMessage == null) {
					// 但不排除这个，我就看到过一条log_msg为null的记录
					// 当为null时后面的操作就没必要了，完全可以跳过去检查下一条
					continue;
				}

				JSONObject jsonObject = null;
				String event = null;
				try {
					boolean isReadGson = logMessage.startsWith("[");
					if (isReadGson) {
						try {
							jsonObject = GsonHelper.dealGson2Json(logMessage);
						} catch (ParseJsonException e) {
							logger.error(logMessage + "  " + e.getMessage());
							continue;
						}
					} else {
						// 这种情况基本不应该存在了20120525，所以一旦遇到这种情况，应该直接写入库中
						// jsonObject = new JSONObject(logMessage);
						logger.error(logMessage);
						continue;
					}
					if (jsonObject == null) {
						continue;
					}
					String qufuInfo = getQuFu(qufu);
					if (qufuInfo == null) {
						logger.error("区服错误：" + qufu + "   " + logMessage);
						continue;
					}
					jsonObject.put("host", qufuInfo);
					/**
					 * 20120525之前日志json串里没有log_date这个字段，
					 * 对于一些事件就通过数据库的logDate来写的，那个 log4j是异步操作，写入的时间不准
					 */
					// jsonObject.put("log_date", logDate);
					jsonObject.put("mySqlId", idNo);
					event = jsonObject.getString("event");
					if (event == null) {
						logger.error("event 为空   " + logMessage);
						continue;
					}
					/*
					 * key来自配置文件config.properties,用一个方法是为了
					 * 让那些不需要处理的event不写入内存20120525
					 */
					String keyStr = getKeyFromEvent(event);
					if (keyStr != null) {
						// 按这种处理逻辑，下面的链表实际上是进行单线程读操作和单线程写操作
//						rollbackList.add(new RollbackBean(idNo, isDeal));
						likeRedisList.add(new LikeRedisBean(keyStr, jsonObject
								.toString()));
					}
				} catch (JSONException e) {
					logger.error(logMessage);
				}
			}
			System.out.println("读完毕");
			ConnHelper.closeResultSet(rs);
			ConnHelper.closeStmt(pstmt);
			// ConnHelper.closeConn(conn);
			// 记录容错处理
//			UpdateForMdRollback.batchInsert(rollbackList);
			for (LikeRedisBean bean : likeRedisList) {
				// 写入缓冲区
				LikeRedisList.lpush(bean.getKey(), bean.getValue());
			}
//			rollbackList.clear();
			likeRedisList.clear();

			// 写操作=========================================================================
			DealJson2OracleTable dealsTable[] = new DealJson2OracleTable[ReconstructionHelper.eventHelper.length];
			MyThreadsGroup tg = new MyThreadsGroup();
			multiWriteWay.setTg(tg);
			int initThreadsNum = 0;
			for (int i = 0; i < ReconstructionHelper.eventName.length; i++) {
				if (ReconstructionHelper.IS_DEAL[i] == 1) {
					dealsTable[i] = factory
							.getDealTable(ReconstructionHelper.eventName[i]);
					MultiWriteThread t = new MultiWriteThread(dealsTable[i]);
					t.setWriteOverListener(multiWriteWay);
					// t.start();
					tg.add(t);
					initThreadsNum++;
				}
			}
			initThreadsNum = initThreadsNum > 5 ? 5 : initThreadsNum;
			// 先产生5个写的线程
			/**
			 * 程序老自己就死锁了，一直没找到原因，现在总算找到了，原来是：
			 * pop后就启动了线程，而那个线程结束的时候可能wait()还没调用，或者二者一直抢资源
			 * 
			 */
			for (int i = 0; i < 5; i++) {
				tg.pop();
			}

			synchronized (multiWriteWay.getLockObj()) {
				try {
					System.out.println("i am waitting ..... ");
					iamWaitting = true;
					multiWriteWay.getLockObj().wait();
					iamWaitting = false;
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			// fromId增加块
			fromId += ConfigHelper.BLOCK_SIZE;
		}
		conn.close();
	}
	
	public static boolean iamWaitting = false;

	private static int getMinId(Connection conn) throws SQLException {
		PreparedStatement pstmtGetMinId = conn
				.prepareStatement("select min(id) from "
						+ ConfigHelper.TBALE_NAME + " where log_date <= '"
						+ ConfigHelper.TO_DATE + "' and log_date >= '"
						+ ConfigHelper.FROM_DATE + "'");
		ResultSet rs = pstmtGetMinId.executeQuery();
		int minId = 0;
		if (rs.next()) {
			minId = rs.getInt(1);
		}
		pstmtGetMinId.close();
		rs.close();
		return minId;
	}

	private static int getMaxId(Connection conn) throws SQLException {
		PreparedStatement pstmtGetMaxId = conn
				.prepareStatement("select max(id) from "
						+ ConfigHelper.TBALE_NAME + " where log_date <= '"
						+ ConfigHelper.TO_DATE + "' and log_date >= '"
						+ ConfigHelper.FROM_DATE + "'");
		ResultSet rs = pstmtGetMaxId.executeQuery();
		int maxId = 0;
		if (rs.next()) {
			maxId = rs.getInt(1);
		}
		pstmtGetMaxId.close();
		rs.close();
		return maxId;
	}

	private static String getKeyFromEvent(String event) {
		for (int i = 0; i < ReconstructionHelper.eventName.length; i++) {
			/**
			 * 由equals改为了startWith，因为yuanbao_incr很多类型都加了新的后缀，而yuanbao
			 * 日志该如何写还没确定下来，是单独写一个游戏库还是由日志的形式写，还待商榷，如此 此种方法是临时方法
			 */
			if (event.startsWith(ReconstructionHelper.eventName[i])
					&& ReconstructionHelper.IS_DEAL[i] == 1) {
				return ReconstructionHelper.eventName[i];
			}
		}
		return null;
	}

	// 区服处理
	public static String getQuFu(String qufu) {
		//在没上三位数前暂且如此这般
		qufu = qufu.substring(qufu.length() - 2,qufu.length());
		return GmConfig.qufuMapper_r.get(Integer.parseInt(qufu));
	}
}
