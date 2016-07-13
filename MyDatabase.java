import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.RandomAccessFile;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;

import au.com.bytecode.opencsv.CSVReader;

public class MyDatabase {
	static byte double_blind_mask      = 8;    // binary 0000 1000
	static byte controlled_study_mask  = 4;    // binary 0000 0100
	static byte govt_funded_mask       = 2;    // binary 0000 0010
	static byte fda_approved_mask      = 1;    // binary 0000 0001
	static byte deleted_mask		   = (byte)(127);  // binary 0111 1111
	static byte delete_flag			   = (byte) (deleted_mask ^ 0xFF);
	byte commonByte = 0x00; 
	final static String file_op_location="C:\\Users\\Silpy\\Project\\";
	int no = 0;

	RandomAccessFile file = null;
	static String fileName;
	
	Map<Integer,Integer> idIndex = new TreeMap<Integer,Integer>();
	Map<String,String> companyIndex = new TreeMap<String,String>();
	Map<String,String> drugIndex = new TreeMap<String,String>();		
	Map<Integer,String> trialsIndex = new TreeMap<Integer,String>();
	Map<Integer,String> patientsIndex = new TreeMap<Integer,String>();
	Map<Integer,String> dosageIndex = new TreeMap<Integer,String>();
	Map<Float,String> readingIndex = new TreeMap<Float,String>();
	Map<String,String> dBIndex = new TreeMap<String,String>();
	Map<String,String> csIndex = new TreeMap<String,String>();
	Map<String,String> gfIndex = new TreeMap<String,String>();
	Map<String,String> fdaIndex = new TreeMap<String,String>();
	
	StringBuffer buffer = new StringBuffer();
	DataOutputStream idOut=null;
	DataOutputStream companyOut=null;
	DataOutputStream drugOut=null;
	DataOutputStream trialsOut=null;
	DataOutputStream patientsOut=null;
	DataOutputStream dosageOut=null;
	DataOutputStream readingOut=null;
	DataOutputStream dBOut=null;
	DataOutputStream csOut=null;
	DataOutputStream gfOut=null;
	DataOutputStream fdaOut=null;
	
	static int filesize=0;
	
public void csvtobinary(String fileName){
	
	this.fileName = fileName;	
	
	try{
		
		
		file = new RandomAccessFile(file_op_location+fileName+".db","rw");
		CSVReader readFile = new CSVReader(new FileReader(file_op_location+fileName+".csv"));
		
		idOut = new DataOutputStream(new FileOutputStream(file_op_location+fileName+".id.ndx"));
		companyOut = new DataOutputStream(new FileOutputStream(file_op_location+fileName+".company.ndx"));
		drugOut = new DataOutputStream(new FileOutputStream(file_op_location+fileName+".drug_id.ndx"));
		trialsOut = new DataOutputStream(new FileOutputStream(file_op_location+fileName+".trials.ndx"));
		patientsOut = new DataOutputStream(new FileOutputStream(file_op_location+fileName+".patients.ndx"));
		dosageOut = new DataOutputStream(new FileOutputStream(file_op_location+fileName+".dosage_mg.ndx"));
		readingOut = new DataOutputStream(new FileOutputStream(file_op_location+fileName+".reading.ndx"));
		dBOut = new DataOutputStream(new FileOutputStream(file_op_location+fileName+".double_blind.ndx"));
		csOut = new DataOutputStream(new FileOutputStream(file_op_location+fileName+".controlled_study.ndx"));
		gfOut = new DataOutputStream(new FileOutputStream(file_op_location+fileName+".govt_funded.ndx"));
		fdaOut = new DataOutputStream(new FileOutputStream(file_op_location+fileName+".fda_approved.ndx"));
		
		String token[];
				
		while((token=readFile.readNext())!=null){
			if(this.no==0){
			++this.no;
			continue;
			}
			//ID field
			int id=Integer.parseInt(token[0]);
			if(idIndex.get(id)==null)
				idIndex.put(id, filesize);
			
			file.writeInt(id);
			filesize+=4;
			//Company field
			if(companyIndex.get(token[1])==null)
				companyIndex.put(token[1], id+"");
			else
				companyIndex.put(token[1],companyIndex.get(token[1])+"%"+id+"");
			
			int companyLength = token[1].length();
			file.writeByte(companyLength);
			filesize+=1;
			
			char[] company = token[1].toCharArray();

			for (int i = 0; i < company.length; i++)
			{     	            	  
				file.writeByte(company[i]);  
				buffer.append(String.format("%8s", Integer.toBinaryString((int) company[i])).replace(' ', '0'));
			}
			filesize+=companyLength;
			//DrugId field
			if(drugIndex.get(token[2])==null)
				drugIndex.put(token[2], id+"");
			else
				drugIndex.put(token[2],drugIndex.get(token[2])+"%"+id+"");
			
			char[] drug = token[2].toCharArray();
			for (int i = 0; i < drug.length; i++)
			{     	            	  

				file.writeByte(drug[i]);  
				buffer.append(String.format("%8s", Integer.toBinaryString((int) drug[i])).replace(' ', '0'));
			}
			filesize+=6;
			//Trials field
			int trials = Integer.parseInt(token[3]);
			if(trialsIndex.get(trials)==null)
				trialsIndex.put(trials, id+"");
			else
				trialsIndex.put(trials,trialsIndex.get(trials)+"%"+id+"");
			
			file.writeShort(trials);
			filesize+=2;
			//patients field
			int patients = Integer.parseInt(token[4]);
			if(patientsIndex.get(patients)==null)
				patientsIndex.put(patients, id+"");
			else
				patientsIndex.put(trials,patientsIndex.get(patients)+"%"+id+"");
			
			file.writeShort(patients);
			filesize+=2;
			//Doasge field
			int dosage = Integer.parseInt(token[5]);
			if(dosageIndex.get(dosage)==null)
				dosageIndex.put(dosage, id+"");
			else
				dosageIndex.put(dosage,dosageIndex.get(dosage)+"%"+id+"");
			
			file.writeShort(dosage);
			filesize+=2;
			//Reading field
			String reading = token[6];			
			float read_float = Float.valueOf(reading).floatValue();
			
			if(readingIndex.get(read_float)==null)
				readingIndex.put(read_float, id+"");
			else
				readingIndex.put(read_float,readingIndex.get(read_float)+"%"+id+"");
			
			file.writeFloat(read_float);	
			filesize+=4;

			//double_blind field
			if("true".equalsIgnoreCase(token[7]))
				double_blind_mask=8;
			else
				double_blind_mask=0;
			
			if(dBIndex.get(token[7])==null)
				dBIndex.put(token[7], id+"");
			else
				dBIndex.put(token[7],dBIndex.get(token[7])+"%"+id+"");

			//controlled_study field
			if("true".equalsIgnoreCase(token[8]))
				controlled_study_mask=4;
			else
				controlled_study_mask=0;

			if(csIndex.get(token[8])==null)
				csIndex.put(token[8], id+"");
			else
				csIndex.put(token[8],csIndex.get(token[8])+"%"+id+"");

			//govt_funded field
			if("true".equalsIgnoreCase(token[9]))
				govt_funded_mask=2;
			else
				govt_funded_mask=0;

			if(gfIndex.get(token[9])==null)
				gfIndex.put(token[9], id+"");
			else
				gfIndex.put(token[9],gfIndex.get(token[9])+"%"+id+"");

			//fda_approved field
			if("true".equalsIgnoreCase(token[10]))
				fda_approved_mask=1;
			else
				fda_approved_mask=0;

			if(fdaIndex.get(token[10])==null)
				fdaIndex.put(token[10], id+"");
			else
				fdaIndex.put(token[10],fdaIndex.get(token[10])+"%"+id+"");

			commonByte = (byte)(commonByte | double_blind_mask);
			commonByte = (byte)(commonByte | controlled_study_mask);
			commonByte = (byte)(commonByte | govt_funded_mask);
			commonByte = (byte)(commonByte | fda_approved_mask);

			file.writeByte(commonByte);
			filesize+=1;
			++this.no;
			//readFile.close();
		}
		// id ndx write
			Iterator itr = idIndex.entrySet().iterator();
			StringBuffer buff = new StringBuffer();

			while (itr.hasNext()) {
				Map.Entry entry = (Map.Entry)itr.next();
				System.out.println(entry.getKey() + " = " + entry.getValue());
				buff.append(entry.getKey()+","+entry.getValue());
				buff.append("\n");
			}			
			idOut.writeBytes(buff.toString());
			
			// company ndx write
			itr = companyIndex.entrySet().iterator();
			buff = new StringBuffer();

			while (itr.hasNext()) {
				Map.Entry entry = (Map.Entry)itr.next();
				System.out.println(entry.getKey() + " = " + entry.getValue());
				buff.append(entry.getKey()+"="+entry.getValue());
				buff.append("\n");
			}			
			companyOut.writeBytes(buff.toString());
			
			// drug ndx write
			itr = drugIndex.entrySet().iterator();
			buff = new StringBuffer();

			while (itr.hasNext()) {
				Map.Entry entry = (Map.Entry)itr.next();
				System.out.println(entry.getKey() + " = " + entry.getValue());
				buff.append(entry.getKey()+"="+entry.getValue());
				buff.append("\n");
			}			
			drugOut.writeBytes(buff.toString());
			
			// trials ndx write
			itr = trialsIndex.entrySet().iterator();
			buff = new StringBuffer();

			while (itr.hasNext()) {
				Map.Entry entry = (Map.Entry)itr.next();
				System.out.println(entry.getKey() + " = " + entry.getValue());
				buff.append(entry.getKey()+"="+entry.getValue());
				buff.append("\n");
			}			
			trialsOut.writeBytes(buff.toString());
			
			// patients ndx write
			itr = patientsIndex.entrySet().iterator();
			buff = new StringBuffer();

			while (itr.hasNext()) {
				Map.Entry entry = (Map.Entry)itr.next();
				System.out.println(entry.getKey() + " = " + entry.getValue());
				buff.append(entry.getKey()+"="+entry.getValue());
				buff.append("\n");
			}			
			patientsOut.writeBytes(buff.toString());
			
			// dosage ndx write
			itr = dosageIndex.entrySet().iterator();
			buff = new StringBuffer();

			while (itr.hasNext()) {
				Map.Entry entry = (Map.Entry)itr.next();
				System.out.println(entry.getKey() + " = " + entry.getValue());
				buff.append(entry.getKey()+"="+entry.getValue());
				buff.append("\n");
			}			
			dosageOut.writeBytes(buff.toString());
			
			// reading ndx write
			itr = readingIndex.entrySet().iterator();
			buff = new StringBuffer();

			while (itr.hasNext()) {
				Map.Entry entry = (Map.Entry)itr.next();
				System.out.println(entry.getKey() + " = " + entry.getValue());
				buff.append(entry.getKey()+"="+entry.getValue());
				buff.append("\n");
			}			
			readingOut.writeBytes(buff.toString());
			
			// double_blind ndx write
			itr = dBIndex.entrySet().iterator();
			buff = new StringBuffer();

			while (itr.hasNext()) {
				Map.Entry entry = (Map.Entry)itr.next();
				System.out.println(entry.getKey() + " = " + entry.getValue());
				buff.append(entry.getKey()+"="+entry.getValue());
				buff.append("\n");
			}			
			dBOut.writeBytes(buff.toString());
			
			// controlled_study ndx write
			itr = csIndex.entrySet().iterator();
			buff = new StringBuffer();

			while (itr.hasNext()) {
				Map.Entry entry = (Map.Entry)itr.next();
				System.out.println(entry.getKey() + " = " + entry.getValue());
				buff.append(entry.getKey()+"="+entry.getValue());
				buff.append("\n");
			}			
			csOut.writeBytes(buff.toString());
			
			// govt_funded ndx write
			itr = gfIndex.entrySet().iterator();
			buff = new StringBuffer();

			while (itr.hasNext()) {
				Map.Entry entry = (Map.Entry)itr.next();
				System.out.println(entry.getKey() + " = " + entry.getValue());
				buff.append(entry.getKey()+"="+entry.getValue());
				buff.append("\n");
			}			
			gfOut.writeBytes(buff.toString());
			
			// fda_approved ndx write
			itr = fdaIndex.entrySet().iterator();
			buff = new StringBuffer();

			while (itr.hasNext()) {
				Map.Entry entry = (Map.Entry)itr.next();
				System.out.println(entry.getKey() + " = " + entry.getValue());
				buff.append(entry.getKey()+"="+entry.getValue());
				buff.append("\n");
			}			
			fdaOut.writeBytes(buff.toString());
			
			System.out.println("count is: "+this.no);
			System.out.println("Size :"+filesize);
	}
	catch(Exception e){
		e.printStackTrace();		
	}
	finally
	{
		try {
			file.close();
			/*idOut.close();
			companyOut.close();
			drugOut.close();
			trialsOut.close();
			patientsOut.close();
			dosageOut.close();
			readingOut.close();
			dBOut.close();
			csOut.close();
			gfOut.close();
			fdaOut.close();*/
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
 }

public void deleteId(int id){
	StringBuffer buffer = new StringBuffer();
		String fileIndex = file_op_location+fileName+".id.ndx";
		  		
		try{
			RandomAccessFile file = new RandomAccessFile(file_op_location+fileName+".db","rw");
			FileInputStream fis= new FileInputStream(fileIndex);
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));
			
		String lineReqd = null;
		int j=0;
		while(j!=id){
			lineReqd=br.readLine();
			++j;
		}			
		//lineReqd = br.readLine();
		String arr[]=lineReqd.split(",");
		int count  = 0;
		
		int bytesToSkip = Integer.parseInt(arr[1]);
		file.seek(bytesToSkip);
		
		int id1=file.readInt();
		count = bytesToSkip + 5;
		int size = file.read();
		
		byte[] company = new byte[size];
		for(int i=0;i<size;i++)
			company[i]=(byte) file.read();

		String companyName = new String(company);
		count += size;
		byte[] drug = new byte[6];
		for(int i=0;i<6;i++)
			drug[i]=(byte) file.read();

		String drugName = new String(drug);
		count +=6;

		int trials = file.readShort();
		count +=2;
		int patients = file.readShort();
		count +=2;
		int dosage = file.readShort();
		count +=2;
		float reading = file.readFloat();	
		count +=4;
		byte commonByte = file.readByte();
		file.seek(count);
		commonByte=(byte) (commonByte|delete_flag);
		file.writeByte(commonByte);
		System.out.println("Record Deleted");
		}
		catch(Exception e){
			e.printStackTrace();
		}
	
}
  	@SuppressWarnings("finally")
	public String queryId(int id){
  		
  		StringBuffer buffer = new StringBuffer();
  		String fileIndex = file_op_location+fileName+".id.ndx";
  		  		
  		try{
  			RandomAccessFile file = new RandomAccessFile(file_op_location+fileName+".db","r");
  			FileInputStream fis= new FileInputStream(fileIndex);
			BufferedReader br = new BufferedReader(new InputStreamReader(fis));
  			
			String lineReqd = null;
			int j=0;
			while(j!=id){
				lineReqd=br.readLine();
				++j;
			}			
			//lineReqd = br.readLine();
			String arr[]=lineReqd.split(",");
			
			int bytesToSkip = Integer.parseInt(arr[1]);
			file.seek(bytesToSkip);
			
			int id1=file.readInt();
			int size = file.read();
						
			byte[] company = new byte[size];
			for(int i=0;i<size;i++)
				company[i]=(byte) file.read();

			String companyName = new String(company);
			
			byte[] drug = new byte[6];
			for(int i=0;i<6;i++)
				drug[i]=(byte) file.read();

			String drugName = new String(drug);

			int trials = file.readShort();
			
			int patients = file.readShort();
			
			int dosage = file.readShort();
			
			float reading = file.readFloat();
			
			byte commonByte = file.readByte();
						
			//print part
			boolean flag=true;
			if((commonByte & delete_flag)==0)
				flag=true;
			else
				flag=false;	
			
			//String records = printHeaders();
			
			if(flag){
				
			System.out.print(id1);
			System.out.print("  ");		
			System.out.print(companyName);

			for(int i=0;i<45-size;i++)
				System.out.print(" ");
			System.out.print(drugName);
			System.out.print("  ");

			System.out.print(trials);
			System.out.print("      ");
			
			System.out.print(patients);
			System.out.print("      ");
			
			System.out.print(dosage);
			System.out.print("      ");
					
			System.out.print(reading);
			System.out.print("      ");
			
			if((commonByte & double_blind_mask)==0)
			{
				System.out.print("FALSE");
				System.out.print("        ");
			}
			else
			{
				System.out.print("TRUE");
				System.out.print("         ");

			}

			if((commonByte & controlled_study_mask)==0)
			{
				System.out.print("FALSE");
				System.out.print("       ");
			}
			else
			{
				System.out.print("TRUE");
				System.out.print("      ");
			}

			if((commonByte & govt_funded_mask)==0)
			{
				System.out.print("FALSE");
				System.out.print("      ");
			}
			else
			{
				System.out.print("TRUE");
				System.out.print("      ");
			}

			if((commonByte & fda_approved_mask)==0)
				System.out.print("FALSE");
			else
				System.out.print("TRUE");
			System.out.println("");	
			}
        }
  		catch(Exception e){
  			e.printStackTrace();
  		}
  		finally{
  		}
  		return buffer.toString();
	
    }
  	
	public void queryStringField(String fieldName,String fieldValue,String flag) throws Exception
	{
		BufferedReader br =null;
		fieldValue=fieldValue.trim();
		fieldValue=fieldValue.substring(1,fieldValue.length()-1);
		  			
		try
		{
			String fileIndex = file_op_location+fileName+"."+fieldName+".ndx";
			//file = new RandomAccessFile(file_op_location+fileName+".db","rw");
			FileInputStream fis= new FileInputStream(fileIndex);
			br = new BufferedReader(new InputStreamReader(fis));

			String lineReqd ="";
			String nextLine="";

			while((nextLine=br.readLine())!=null)
			{

				String nextLineArr[] = nextLine.split("=");
				if(fieldValue.equalsIgnoreCase(nextLineArr[0]))
				{
					lineReqd=nextLine;
					break;
				}
			}

			String[] data = lineReqd.split("=");
			String[] id = data[1].split("%");

			for(int j=0;j<id.length;j++)
			{
				if(flag.equals("D"))
				 deleteId(Integer.parseInt(id[j]));
				else
				 queryId(Integer.parseInt(id[j]));
			}
		}
		catch(Exception e){
				e.printStackTrace();
			}
		}
	
	public void queryNumericField(String NumfieldName,int numValue,String flag) throws Exception
	{
		BufferedReader br =null;
		try
		{
			String fileIndex = file_op_location+fileName+"."+NumfieldName+".ndx";
			//file = new RandomAccessFile(file_op_location+fileName+".db","rw");
			FileInputStream fis= new FileInputStream(fileIndex);
			br = new BufferedReader(new InputStreamReader(fis));

			String lineReqd ="";
			String nextLine="";

			while((nextLine=br.readLine())!=null)
			{

				String nextLineArr[] = nextLine.split("=");
				if(numValue == Integer.parseInt(nextLineArr[0].trim()))
				{
					lineReqd=nextLine;
					break;
				}
			}

			String[] data = lineReqd.split("=");
			String[] id = data[1].split("%");

			for(int j=0;j<id.length;j++)
			{
				if(flag.equals("D"))
					deleteId(Integer.parseInt(id[j]));
					else
					queryId(Integer.parseInt(id[j]));
			}
		}
		catch(Exception e){
				e.printStackTrace();
			}
		}
	
	public void queryFloatField(String fieldName,float value, String operator,String flag) throws Exception
	{

		FileInputStream fs= null;
		BufferedReader br =null;

		try
		{
			String fileIndex = file_op_location+fileName+"."+fieldName+".ndx";
			//file = new RandomAccessFile(file_op_location+fileName+".db","rw");
			FileInputStream fis= new FileInputStream(fileIndex);
			br = new BufferedReader(new InputStreamReader(fis));

			String lineReqd ="";
			String nextLine="";

			while((nextLine=br.readLine())!=null)
			{

				String nextLineArr[] = nextLine.split("=");
				if(operator.equals("="))
				{
					if(value == Float.parseFloat(nextLineArr[0]))
						lineReqd=nextLine;

					if(lineReqd!=null && !("".equals(lineReqd)))
					{
						String[] data = lineReqd.split("=");
						String[] id = data[1].split("%");

						for(int j=0;j<id.length;j++)
						{
							if(flag.equals("D"))
							deleteId(Integer.parseInt(id[j]));
							else
							queryId(Integer.parseInt(id[j]));
						}
						break;
					}


				}

				else if(operator.equals(">"))
				{

					if(Float.parseFloat(nextLineArr[0])>value)
						lineReqd=nextLine;

					if(lineReqd!=null && !("".equals(lineReqd)))
					{
						String[] data = lineReqd.split("=");
						String[] id = data[1].split("%");

						for(int j=0;j<id.length;j++)
						{
							if(flag.equals("D"))
								deleteId(Integer.parseInt(id[j]));
								else
								queryId(Integer.parseInt(id[j]));
						}
					}

				}

				else if(operator.equals(">="))
				{

					if(Float.parseFloat(nextLineArr[0])>=value)
						lineReqd=nextLine;

					if(lineReqd!=null && !("".equals(lineReqd)))
					{
						String[] data = lineReqd.split("=");
						String[] id = data[1].split("%");

						for(int j=0;j<id.length;j++)
						{
							if(flag.equals("D"))
								deleteId(Integer.parseInt(id[j]));
								else
								queryId(Integer.parseInt(id[j]));
						}
					}


				}

				else if(operator.equals("<"))
				{

					if(Float.parseFloat(nextLineArr[0])<value)
						lineReqd=nextLine;


					if(lineReqd!=null && !("".equals(lineReqd)))
					{
						String[] data = lineReqd.split("=");
						String[] id = data[1].split("%");

						for(int j=0;j<id.length;j++)
						{
							if(flag.equals("D"))
								deleteId(Integer.parseInt(id[j]));
								else
								queryId(Integer.parseInt(id[j]));
						}
					}


				}

				else if(operator.equals("<="))
				{

					if(Float.parseFloat(nextLineArr[0])<=value)
						lineReqd=nextLine;


					if(lineReqd!=null && !("".equals(lineReqd)))
					{
						String[] data = lineReqd.split("=");
						String[] id = data[1].split("%");

						for(int j=0;j<id.length;j++)
						{
							if(flag.equals("D"))
								deleteId(Integer.parseInt(id[j]));
								else
								queryId(Integer.parseInt(id[j]));
						}
					}


				}

				else if(operator.equals("!="))
				{

					if(Float.parseFloat(nextLineArr[0])!=value)
						lineReqd=nextLine;


					if(lineReqd!=null && !("".equals(lineReqd)))
					{
						String[] data = lineReqd.split("=");
						String[] id = data[1].split("%");

						for(int j=0;j<id.length;j++)
						{
							if(flag.equals("D"))
								deleteId(Integer.parseInt(id[j]));
								else
								queryId(Integer.parseInt(id[j]));
						}
					}

				}

			}

		}
		catch(Exception e){
			e.printStackTrace();
		}

	}
	
	public String printHeaders(String select){
		if(select.equals("*")){
			select = "id,company,drug_id,trials,patients,dosage_mg,reading,double_blind,controlled_study,govt_funded,fda_approved";
			System.out.println("Id   Company         Drug_Id  Trials  Patients  Dosage_Mg  Reading  Double_Blind Controlled_Study Govt_Funded FDA_Approved");
		}
		else{
			String select_recs[] = select.split(",");
			for(int i=0;i<select_recs.length;i++){
				System.out.print(select_recs[i] + "   ");
			}
			System.out.println();
		}
		return select;
	}
	public void select_records(String dbname,String select_clause,String where_clause,String flag){
		String pattern = ("[=!<>]*");
		String split[] = where_clause.split(" ");
		// NOT Logic
		String[] opArr = {"<=",">=","<",">","="};
		String[] notArr = {">","<",">=","<=","!="};
		String operator;
		int j=0;
		if(where_clause.contains("not")){
			for(j=0;j<opArr.length;j++){
				if(where_clause.contains(opArr[j])){
					where_clause = (where_clause.replace("not", "").replace(opArr[j], notArr[j])).replace(" ", " ");
					break;
				}
			}
			operator = notArr[j];
		}
		else
		{
			operator = split[1].trim();
		}
		where_clause = where_clause.replaceAll(pattern, "").trim();
		//String field_name= where_clause.substring(0, where_clause.indexOf(" ")).trim();
		String field_value = where_clause.substring(where_clause.indexOf(" ")).trim(); 
		String field_name= split[0].trim();
		//String field_value = split[2].trim(); 
		String p = printHeaders(select_clause);
		
		// NOT logic
		
		
		if(field_name.equals("id")){
			int value = Integer.parseInt(field_value);
			if(operator.equals("=")){
				if(flag.equals("D"))
					deleteId(value);
				else
					queryId(value);
			}
			else if (operator.equals(">")){
				
				for(int i=value+1;i<max_id();i++){
					if(flag.equals("D"))
						deleteId(i);
					else
					    queryId(i);
				}
			}
			else if(operator.equals(">="))
			{

				for(int i=value;i<max_id();i++)
				{
					if(flag.equals("D"))
						deleteId(i);
					else
						queryId(i);
				}

			}

			else if(operator.equals("<"))
			{

				for(int i=1;i<value;i++)
				{
					if(flag.equals("D"))
						deleteId(i);
					else
					queryId(i);
				}

			}

			else if(operator.equals("<="))
			{

				for(int i=1;i<=value;i++)
				{
					if(flag.equals("D"))
						deleteId(i);
					else
					queryId(i);
				}

			}

			else if(operator.equals("!="))
			{

				for(int i=1;i<max_id() && i!=value;i++)
				{
					if(flag.equals("D"))
						deleteId(i);
					else
					queryId(i);
				}
			}
		}
			else if (field_name.equals("company") || field_name.equals("controlled_study") || field_name.equals("double_blind")||
					field_name.equals("govt_funded") || field_name.equals("fda_approved") || field_name.equals("drug_id")){
				try {
					if(flag.equals("D"))
					queryStringField(field_name,field_value,"D");
					else
					queryStringField(field_name,field_value,"S");	
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			else if (field_name.equals("trials") || field_name.equals("patients")||field_name.equals("dosage_mg")){
				try {
					int int_value = Integer.parseInt(field_value);
					if(flag.equals("D"))
					   queryNumericField(field_name,int_value,"D");
					else
						queryNumericField(field_name,int_value,"S");
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			else if (field_name.equals("reading")){
				try {
					float f = Float.parseFloat(field_value);
					if(flag.equals("D"))
					   queryFloatField(field_name,f,operator,"D");
					else
					   queryFloatField(field_name,f,operator,"S");
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

	}
	
	public void insert_record(String filename,String fields){
		// check if index present
		String token[]=fields.split("','");
		
		int id = Integer.parseInt(token[0]);
  		StringBuffer buffer = new StringBuffer();
  		String fileIndex = file_op_location+filename+".id.ndx";
  		  		
  		try{
  			FileInputStream fis= new FileInputStream(fileIndex);
			BufferedReader br = new BufferedReader(new InputStreamReader(fis));
  			
			//String lineReqd = null;
			boolean flag = false;
			int j=1;
			while(br.readLine()!=null){			
				if(j == id){
					flag = true;
					break;
				}
				++j;
			}	
			if(flag)			
			System.out.println("Data for the given Id already present");
			else{
				file = new RandomAccessFile(file_op_location+fileName+".db","rw");
				//ID field
					id=Integer.parseInt(token[0]);
					if(idIndex.get(id)==null)
						idIndex.put(id, filesize);					
					//file.skipBytes(filesize);
					file.seek(filesize);
					file.writeInt(id);
					filesize+=4;
					
					//Company field
					if(companyIndex.get(token[1])==null)
						companyIndex.put(token[1], id+"");
					else
						companyIndex.put(token[1],companyIndex.get(token[1])+"%"+id+"");
					
					int companyLength = token[1].length();
					file.writeByte(companyLength);
					filesize+=1;
					
					char[] company = token[1].toCharArray();

					for (int i = 0; i < company.length; i++)
					{     	            	  
						file.writeByte(company[i]);  
						buffer.append(String.format("%8s", Integer.toBinaryString((int) company[i])).replace(' ', '0'));
					}
					filesize+=companyLength;
					//DrugId field
					if(drugIndex.get(token[2])==null)
						drugIndex.put(token[2], id+"");
					else
						drugIndex.put(token[2],drugIndex.get(token[2])+"%"+id+"");
					
					char[] drug = token[2].toCharArray();
					for (int i = 0; i < drug.length; i++)
					{     	            	  

						file.writeByte(drug[i]);  
						buffer.append(String.format("%8s", Integer.toBinaryString((int) drug[i])).replace(' ', '0'));
					}
					filesize+=6;
					//Trials field
					int trials = Integer.parseInt(token[3]);
					if(trialsIndex.get(trials)==null)
						trialsIndex.put(trials, id+"");
					else
						trialsIndex.put(trials,trialsIndex.get(trials)+"%"+id+"");
					
					file.writeShort(trials);
					filesize+=2;
					//patients field
					int patients = Integer.parseInt(token[4]);
					if(patientsIndex.get(patients)==null)
						patientsIndex.put(patients, id+"");
					else
						patientsIndex.put(trials,patientsIndex.get(patients)+"%"+id+"");
					
					file.writeShort(patients);
					filesize+=2;
					//Doasge field
					int dosage = Integer.parseInt(token[5]);
					if(dosageIndex.get(dosage)==null)
						dosageIndex.put(dosage, id+"");
					else
						dosageIndex.put(dosage,dosageIndex.get(dosage)+"%"+id+"");
					
					file.writeShort(dosage);
					filesize+=2;
					//Reading field
					String reading = token[6];			
					float read_float = Float.valueOf(reading).floatValue();
					
					if(readingIndex.get(read_float)==null)
						readingIndex.put(read_float, id+"");
					else
						readingIndex.put(read_float,readingIndex.get(read_float)+"%"+id+"");
					
					file.writeFloat(read_float);	
					filesize+=4;

					//double_blind field
					if("true".equalsIgnoreCase(token[7]))
						double_blind_mask=8;
					else
						double_blind_mask=0;
					
					if(dBIndex.get(token[7])==null)
						dBIndex.put(token[7], id+"");
					else
						dBIndex.put(token[7],dBIndex.get(token[7])+"%"+id+"");

					//controlled_study field
					if("true".equalsIgnoreCase(token[8]))
						controlled_study_mask=4;
					else
						controlled_study_mask=0;

					if(csIndex.get(token[8])==null)
						csIndex.put(token[8], id+"");
					else
						csIndex.put(token[8],csIndex.get(token[8])+"%"+id+"");

					//govt_funded field
					if("true".equalsIgnoreCase(token[9]))
						govt_funded_mask=2;
					else
						govt_funded_mask=0;

					if(gfIndex.get(token[9])==null)
						gfIndex.put(token[9], id+"");
					else
						gfIndex.put(token[9],gfIndex.get(token[9])+"%"+id+"");

					//fda_approved field
					if("true".equalsIgnoreCase(token[10]))
						fda_approved_mask=1;
					else
						fda_approved_mask=0;

					if(fdaIndex.get(token[10])==null)
						fdaIndex.put(token[10], id+"");
					else
						fdaIndex.put(token[10],fdaIndex.get(token[10])+"%"+id+"");

					commonByte = (byte)(commonByte | double_blind_mask);
					commonByte = (byte)(commonByte | controlled_study_mask);
					commonByte = (byte)(commonByte | govt_funded_mask);
					commonByte = (byte)(commonByte | fda_approved_mask);

					file.writeByte(commonByte);
					++this.no;
				
				Iterator itr = idIndex.entrySet().iterator();				
				String file_ind= file_op_location+fileName+".id.ndx";
			    FileWriter fw = new FileWriter(file_ind,true); 			    
				while (itr.hasNext()) {
					Map.Entry entry = (Map.Entry)itr.next();
					System.out.println(entry.getKey() + " = " + entry.getValue());
					fw.write(entry.getKey()+","+entry.getValue()+"\n");
				}	
				fw.close();
				
				itr = companyIndex.entrySet().iterator();				
				file_ind= file_op_location+fileName+".company.ndx";
			    fw = new FileWriter(file_ind,true);			    
				while (itr.hasNext()) {
					Map.Entry entry = (Map.Entry)itr.next();
					System.out.println(entry.getKey() + " = " + entry.getValue());
					fw.write(entry.getKey()+"="+entry.getValue()+"\n");
				}	
				fw.close();
				
				itr = drugIndex.entrySet().iterator();				
				file_ind= file_op_location+fileName+".drug_id.ndx";
			    fw = new FileWriter(file_ind,true);			    
				while (itr.hasNext()) {
					Map.Entry entry = (Map.Entry)itr.next();
					System.out.println(entry.getKey() + " = " + entry.getValue());
					fw.write(entry.getKey()+"="+entry.getValue()+"\n");
				}	
				fw.close();
				
				itr = trialsIndex.entrySet().iterator();				
				file_ind= file_op_location+fileName+".trials.ndx";
			    fw = new FileWriter(file_ind,true);			    
				while (itr.hasNext()) {
					Map.Entry entry = (Map.Entry)itr.next();
					System.out.println(entry.getKey() + " = " + entry.getValue());
					fw.write(entry.getKey()+"="+entry.getValue()+"\n");
				}	
				fw.close();
				
				itr = patientsIndex.entrySet().iterator();				
				file_ind= file_op_location+fileName+".patients.ndx";
			    fw = new FileWriter(file_ind,true);			    
				while (itr.hasNext()) {
					Map.Entry entry = (Map.Entry)itr.next();
					System.out.println(entry.getKey() + " = " + entry.getValue());
					fw.write(entry.getKey()+"="+entry.getValue()+"\n");
				}	
				fw.close();
				
				itr = dosageIndex.entrySet().iterator();				
				file_ind= file_op_location+fileName+".dosage_mg.ndx";
			    fw = new FileWriter(file_ind,true);			    
				while (itr.hasNext()) {
					Map.Entry entry = (Map.Entry)itr.next();
					System.out.println(entry.getKey() + " = " + entry.getValue());
					fw.write(entry.getKey()+"="+entry.getValue()+"\n");
				}	
				fw.close();
				
				itr = readingIndex.entrySet().iterator();				
				file_ind= file_op_location+fileName+".reading.ndx";
			    fw = new FileWriter(file_ind,true);			    
				while (itr.hasNext()) {
					Map.Entry entry = (Map.Entry)itr.next();
					System.out.println(entry.getKey() + " = " + entry.getValue());
					fw.write(entry.getKey()+"="+entry.getValue()+"\n");
				}	
				fw.close();
				
				itr = dBIndex.entrySet().iterator();				
				file_ind= file_op_location+fileName+".double_blind.ndx";
			    fw = new FileWriter(file_ind,true);			    
				while (itr.hasNext()) {
					Map.Entry entry = (Map.Entry)itr.next();
					System.out.println(entry.getKey() + " = " + entry.getValue());
					fw.write(entry.getKey()+"="+entry.getValue()+"\n");
				}	
				fw.close();
				
				itr = csIndex.entrySet().iterator();				
				file_ind= file_op_location+fileName+".controlled_study.ndx";
			    fw = new FileWriter(file_ind,true);			    
				while (itr.hasNext()) {
					Map.Entry entry = (Map.Entry)itr.next();
					System.out.println(entry.getKey() + " = " + entry.getValue());
					fw.write(entry.getKey()+"="+entry.getValue()+"\n");
				}	
				fw.close();
				
				itr = gfIndex.entrySet().iterator();				
				file_ind= file_op_location+fileName+".govt_funded.ndx";
			    fw = new FileWriter(file_ind,true);			    
				while (itr.hasNext()) {
					Map.Entry entry = (Map.Entry)itr.next();
					System.out.println(entry.getKey() + " = " + entry.getValue());
					fw.write(entry.getKey()+"="+entry.getValue()+"\n");
				}	
				fw.close();
				
				itr = fdaIndex.entrySet().iterator();				
				file_ind= file_op_location+fileName+".fda_approved.ndx";
			    fw = new FileWriter(file_ind,true);			    
				while (itr.hasNext()) {
					Map.Entry entry = (Map.Entry)itr.next();
					System.out.println(entry.getKey() + " = " + entry.getValue());
					fw.write(entry.getKey()+"="+entry.getValue()+"\n");
				}	
				fw.close();
								
				System.out.println("Size :"+filesize);
			System.out.println("Record Inserted");	
			}
  		}
  		catch(Exception e){
  			e.printStackTrace();
  		}
		
	}
	
	public boolean fileExists(String f){
		File f_new = null;
		try{
		 f_new = new File(file_op_location+f+".db");
		 if (f_new.exists()){
			 return true;
		 }
		 else return false;		 
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return false;
	}
	
	public int max_id(){
		String fileIndex = file_op_location+fileName+".id.ndx";
	  		
  		try{
  			FileInputStream fis= new FileInputStream(fileIndex);
			BufferedReader br = new BufferedReader(new InputStreamReader(fis));
  			
			int j=1;
			while(br.readLine()!= null){
				++j;
			}
			return j;
  		}
			catch(Exception e){
				e.printStackTrace();
			}
		return 0;
			
	}
	
	public static void main(String args[]){		
		try{
	while(true){
		
		System.out.println("Please enter a number from below options : ");
		System.out.println("1. Import");
		System.out.println("2. Query");
		System.out.println("3. Insert");
		System.out.println("4. Delete");
		System.out.println("5. Exit");
		
		Scanner scanner = new Scanner(System.in);
		int option = scanner.nextInt();
		String fileName = null;
		
		MyDatabase db = new MyDatabase();
		scanner.nextLine();
		if(option == 1)
		{
			System.out.println("import");
			scanner = new Scanner(System.in);
			fileName=scanner.next();
			fileName = fileName.substring(0, fileName.indexOf(".csv"));
			
			if(db.fileExists(fileName)){
				System.out.println("DB File Already present");
				db.fileName = fileName;
			}
			else{
			db.csvtobinary(fileName);
			}
		}
		else if(option ==2)
		{
			System.out.println("Please enter a query");
			//scanner = new Scanner(System.in);
			//String input = scanner.next();
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			 String input;
			try {
				input = br.readLine();
			String line = input.toLowerCase();
			if(!line.contains("select")&&!line.contains("from"))
				System.out.println("Invalid SQL Syntax");
			else{
			String select_clause = line.substring(line.indexOf("select")+ 6,line.indexOf(" from ")).trim();
			String[] items = select_clause.split(",");
			String from_clause = line.substring(line.indexOf(" from ") + 6, line.indexOf(";")).trim();
            String where_clause = null,dbname=null;
            //String tbl_name=fileName;
            
            if (from_clause.contains(" where ")) {
                where_clause = from_clause.substring(from_clause.indexOf(" where ") + 7);
                dbname = from_clause.substring(0, from_clause.indexOf(" where ")).trim();
            }
            System.out.println("Table Name ==> " + dbname);
            if(db.fileExists(dbname)){
				db.fileName = dbname;
			}
            if (!dbname.equalsIgnoreCase(db.fileName)) {
                System.out.println("Invalid Table Name.");
            }
            
            else{           
            db.select_records(dbname,select_clause,where_clause,"S");
            }
		 }
		}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
		else if(option==3){
			System.out.println("Please enter an insert query");
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			 String input;
			try {
				input = br.readLine();
				 String line = input.toLowerCase();			 
				 String tbl_name = line.substring(line.indexOf(" into ") + 6,line.indexOf(" values")).trim();
				 if(db.fileExists(tbl_name)){
						db.fileName = tbl_name;
					}
				 if (!tbl_name.equalsIgnoreCase(db.fileName)) {
		                System.out.println("Invalid Table Name.");
		            }
				 else{
				 String fields= line.substring(line.indexOf("values ('") + 9,line.indexOf("');")).trim();
				 //String values[]=fields.split("','");
				 db.insert_record(tbl_name,fields);
				 }
			}
			catch(Exception e){
				e.printStackTrace();
			}
			
		}
		else if(option == 4){
			System.out.println("Please enter a delete query");
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			String input;
			try {
				input = br.readLine();
				 String line = input.toLowerCase();	
				 String from_clause = line.substring(line.indexOf(" from ") + 6, line.indexOf(";")).trim();
				 String where_clause = null,dbname=null;
				 String tbl_name = line.substring(line.indexOf(" from ") + 6,line.indexOf(" where")).trim();
				    if(db.fileExists(tbl_name)){
						db.fileName = tbl_name;
					}
				    if (!tbl_name.equalsIgnoreCase(db.fileName)) {
		                System.out.println("Invalid Table Name.");
		            }
				    else{
		            if (from_clause.contains(" where ")) {
		                where_clause = from_clause.substring(from_clause.indexOf(" where ") + 7);
		                dbname = from_clause.substring(0, from_clause.indexOf(" where ")).trim();
		            }
		            db.select_records(tbl_name,"*",where_clause,"D");
				    }
			}
			catch(Exception e){
				e.printStackTrace();
			}
		}
		else if(option==5){
			 System.out.println("Exit!");
             System.exit(0);
             break;
		}
		else{
			System.out.println("Incorrect Option Entered");
			System.out.println("Select Again");
		}
	  }		
	}
		catch(Exception e){
			e.printStackTrace();
		}
		finally{
			System.exit(0);
		}
	
	}
}
