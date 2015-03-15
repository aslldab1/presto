/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.planner.optimizations.estimater;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class LouLoader {

	private static File louFile = new File("etc/lou.properties");
	
	public static Map<String, Double> getLouMap(){
		Properties prop = new Properties();
		Map<String, Double> resultMap = new HashMap<String, Double>();
		try
		{
			InputStream in = new FileInputStream(louFile);
			prop.load(in);
			for(Object key : prop.keySet())
			{
				resultMap.put(key.toString(), Double.parseDouble(prop.get(key).toString()));
			}
		}
		catch(IOException ex)
		{
			ex.printStackTrace();
		}
		return resultMap;
	}
}
