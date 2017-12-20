package houseware.learn.hadoop.mapred.blog.mapreduce.join.repartition;

import houseware.learn.hadoop.mapred.blog.data.writable.ProjectWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class ProjectProjection implements WritableComparable<ProjectWritable> {

    public String project_id;
    public String school_city;
    public String poverty_level;
    public String primary_focus_subject;

    public static ProjectProjection makeProjection(ProjectWritable project) {
        ProjectProjection pp = new ProjectProjection();
        pp.project_id = project.project_id;
        pp.school_city = project.school_city;
        pp.poverty_level = project.poverty_level;
        pp.primary_focus_subject = project.primary_focus_subject;
        return pp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, project_id);
        WritableUtils.writeString(out, school_city);
        WritableUtils.writeString(out, poverty_level);
        WritableUtils.writeString(out, primary_focus_subject);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        project_id = WritableUtils.readString(in);
        school_city = WritableUtils.readString(in);
        poverty_level = WritableUtils.readString(in);
        primary_focus_subject = WritableUtils.readString(in);
    }

    @Override
    public int compareTo(ProjectWritable o) {
        return project_id.compareTo(o.project_id);
    }

}