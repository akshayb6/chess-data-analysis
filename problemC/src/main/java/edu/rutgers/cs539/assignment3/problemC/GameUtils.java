/**
 * 
 */
package edu.rutgers.cs539.assignment3.problemC;

import chesspresso.Chess;

/**
 * @author ashish
 *
 */
public final class GameUtils {
	public static String gameResultToString(int res) {
		if (res == Chess.RES_BLACK_WINS) {
			return "Black";
		} else if (res == Chess.RES_WHITE_WINS) {
			return "White";
		} else if (res == Chess.RES_DRAW) {
			return "Draw";
		} else {
			return "Unknown";
		}
	}
}
