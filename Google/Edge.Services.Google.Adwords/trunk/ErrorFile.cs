using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Edge.Services.Google.Adwords
{
	class ErrorFile
	{
		string _fileName, _path, _headers;
		int _numOfHeaders;
		FileInfo file;
		FileStream stream;

		public ErrorFile(string Name, List<string> headersList, string path)
		{
			_fileName = Name + DateTime.Now.ToString("yyyyMMdd");
			_path = path;
			_headers = CreateHeaders(headersList);
			_numOfHeaders = headersList.Count;
			file = new FileInfo(_path + _fileName);
		}

		public void Open()
		{
			stream = file.Open(FileMode.OpenOrCreate, FileAccess.Write);
		}

		public void AppendToFile<T>(List<T> data)
		{
			using (stream)
			{
				using (StreamWriter sw = new StreamWriter(stream, Encoding.Unicode))
				{
					//Write Headers to file
					sw.WriteLine(_headers);
					StringBuilder _row = new StringBuilder();
					//Write Rows to file
					foreach (T val in data)
					{
						_row.Append(val.ToString());
						_row.Append("\t");
					}
					sw.WriteLine(_row);
					sw.Close();
				}
			}
			stream.Close();
		}

		private string CreateHeaders(List<string> Headers)
		{
			StringBuilder sb = new StringBuilder();
			foreach (var h in Headers)
			{
				sb.Append(h + "\t");
			}
			return sb.ToString();
		}
	}
}
