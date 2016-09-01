import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Administrator on 2016/7/21.
 */
public class DataBean implements Writable {
    private String tel;
    private long upload;
    private long download;
    private long totalload;


    public DataBean(){}

    public DataBean(String tel,long upload,long download )
    {
        this.tel=tel;
        this.upload=upload;
        this.download=download;
        this.totalload=upload+download;
    }


    public String getTel() {
        return tel;
    }

    public void setTel(String tel) {
        this.tel = tel;
    }

    public long getUpload() {
        return upload;
    }

    public void setUpload(long upload) {
        this.upload = upload;
    }

    public long getDownload() {
        return download;
    }

    public void setDownload(long download) {
        this.download = download;
    }

    public long getTotalload() {
        return totalload;
    }

    public void setTotalload(long totalload) {
        this.totalload = totalload;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(tel);
        out.writeLong(upload);
        out.writeLong(download);
        out.writeLong(totalload);
    }

    public void readFields(DataInput in) throws IOException {
        this.tel=in.readUTF();
        this.upload=in.readLong();
        this.download=in.readLong();
        this.totalload=in.readLong();
    }

    @Override
    public String toString() {
        return this.tel+"\t"+this.upload+"\t"+this.download+"\t"+this.totalload;
    }
}
