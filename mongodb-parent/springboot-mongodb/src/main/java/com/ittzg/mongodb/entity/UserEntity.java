package com.ittzg.mongodb.entity;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;
/**
 * @email: tazhigang095@163.com
 * @author: ittzg
 * @date: 2019/7/22 15:12
 */
@Document(collection="user")
public class UserEntity {
    @Id
    private ObjectId id;

    private String username;

    private String sex;

    private AddressEntity address;

    private List<LikeEntity> likes;

    @Document
    public static class AddressEntity{
        private String province;
        private String citiy;
        private String county;

        public String getProvince() {
            return province;
        }

        public void setProvince(String province) {
            this.province = province;
        }

        public String getCitiy() {
            return citiy;
        }

        public void setCitiy(String citiy) {
            this.citiy = citiy;
        }

        public String getCounty() {
            return county;
        }

        public void setCounty(String county) {
            this.county = county;
        }
    }
    @Document
    public static class LikeEntity{
        private String like;

        public String getLike() {
            return like;
        }

        public void setLike(String like) {
            this.like = like;
        }
    }

    public ObjectId getId() {
        return id;
    }

    public void setId(ObjectId id) {
        this.id = id;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public AddressEntity getAddress() {
        return address;
    }

    public void setAddress(AddressEntity address) {
        this.address = address;
    }

    public List<LikeEntity> getLikes() {
        return likes;
    }

    public void setLikes(List<LikeEntity> likes) {
        this.likes = likes;
    }
}
