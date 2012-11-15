require './lib/multi_db/query_analyzer'

describe MultiDb::QueryAnalyzer do

  describe "Table detection" do
    def self.q(sql, *tables)
      specify { subject.tables(sql).should == tables }
    end

    #simple selects
    q("SELECT * FROM `products`", :products)
    q("SELECT * FROM products", :products)
    q("SELECT * from products", :products)
    q("select * from   `products`", :products)
    q("select * from   `with_underscores`", :with_underscores)

    #insert
    q("insert into products (a,b,c) values(1,2,3)", :products)

    #update
    q("update products set id=1", :products)

    #joins
    q("select * from products left join images on products.image_id = images.id", :products, :images)

    #subselect
    q("select * from products where id in (select product_id from images)", :products, :images)

    #multiple tables
    q("select * from products, images", :products, :images)
    q("select * from a,b,c,d,e,f", :a, :b, :c, :d, :e, :f)
    q("select * from a,`b`,c,`d` ,e,f", :a, :b, :c, :d, :e, :f)
  end


  describe "Session stuff" do

    CURRENT_TIME = 1000

    before do
      Speedytime.stub(current: CURRENT_TIME)
    end

    it 'records data writes to the session' do
      session = {}
      subject.record_write_to_session(session, "delete from products")
      session.should == ({
        multi_db: {
          last_write: 1000,
          table_writes: {
            products: 1000
          }
        }
      })
    end

    it "returns Unbounded for queries with no replica lag constraint" do
      session = {}
      subject.max_lag_for_query(session, "select * from products").should == subject::Unbounded
    end

    it "calculates the max replica lag from the last data write" do
      session = {
        multi_db: {
          last_write: 992,
          table_writes: {
            products: 992
          }
        }
      }
      subject.max_lag_for_query(session, "select * from products").should == 10
    end

    it "calculates the max replica lag per table" do
      session = {
        multi_db: {
          last_write: 998,
          table_writes: {
            products: 992,
            orders: 998,
          }
        }
      }
      subject.max_lag_for_query(session, "select * from products").should == 10
      subject.max_lag_for_query(session, "select * from orders").should == 4
    end

    it "records additional writes into an existing session" do
      prev = {
        multi_db: {
          last_write: 999,
          table_writes: {
            baz: 200,
            foobars: 994,
            products: 999
          }
        }
      }
      session = subject.record_write_to_session(prev, "DELETE FROM products, images")
      exp = {
        multi_db: {
          last_write: 1000,
          table_writes: {
            foobars: 994,
            products: 1000,
            images: 1000
          }
        }
      }
      session.should == exp
      subject.max_lag_for_query(session, "SELECT * FROM products").should == 2
      subject.max_lag_for_query(session, "SELECT * FROM foobars").should == 8
    end

  end

end
