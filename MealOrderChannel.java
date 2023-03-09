package com.suanli.order.service.channel;

import cn.hutool.core.convert.Convert;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.suanli.commons.BizException;
import com.suanli.commons.api.ApiResponse;
import com.suanli.commons.model.ClientAppTypeEnum;
import com.suanli.commons.util.IdGenerator;
import com.suanli.commons.util.SessionLocalUtils;
import com.suanli.coupon.api.CouponService;
import com.suanli.coupon.api.CouponUseRelationService;
import com.suanli.coupon.api.UserCouponService;
import com.suanli.coupon.enums.CouponUseRuleType;
import com.suanli.coupon.enums.CouponUseType;
import com.suanli.coupon.enums.UserCouponDeleteStatus;
import com.suanli.coupon.enums.UserCouponUseStatus;
import com.suanli.coupon.model.Coupon;
import com.suanli.coupon.model.CouponUseRelation;
import com.suanli.coupon.model.CouponUserRule;
import com.suanli.coupon.model.UserCoupon;
import com.suanli.order.entity.OrderEntity;
import com.suanli.order.entity.OrderItemEntity;
import com.suanli.order.enums.DeliveryMethodEnum;
import com.suanli.order.enums.OrderDeleteEnum;
import com.suanli.order.enums.OrderStatus;
import com.suanli.order.enums.OrderType;
import com.suanli.order.impl.OrderItemServiceImpl;
import com.suanli.order.impl.OrderServiceImpl;
import com.suanli.order.mapper.OrderMapper;
import com.suanli.order.model.*;
import com.suanli.order.param.PackageOrderItems;
import com.suanli.order.service.OrderChannel;
import com.suanli.order.utils.AttributeUtils;
import com.suanli.product.api.*;
import com.suanli.product.enums.category.CategoryAttributionEnum;
import com.suanli.product.enums.category.CategoryTypeEnum;
import com.suanli.product.enums.item.*;
import com.suanli.product.model.*;
import com.suanli.product.model.param.AttributeValueParam;
import com.suanli.store.api.StoreInfoService;
import com.suanli.store.api.TableBusinessRecordService;
import com.suanli.store.api.TableOrderService;
import com.suanli.store.api.TablePositionService;
import com.suanli.store.enums.tableBusinessRecord.OrderRecordOperationEnum;
import com.suanli.store.enums.tableposition.*;
import com.suanli.store.model.StoreInfo;
import com.suanli.store.model.TableBusinessRecord;
import com.suanli.store.model.TableOrder;
import com.suanli.store.model.TablePosition;
import com.suanli.store.model.param.CreateMealOrderParam;
import org.springframework.stereotype.Component;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;

import javax.annotation.Nonnull;
import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
public class MealOrderChannel implements OrderChannel {

    @Resource
    private SkuService skuService;

    @Resource
    private UserCouponService userCouponService;

    @Resource
    private ItemOtherService itemOtherService;

    @Resource
    private ItemBasicService itemBasicService;

    @Resource
    private SkuInventoryService skuInventoryService;

    @Resource
    private StoreInfoService storeInfoService;

    @Resource
    private AttributeValueService attributeValueService;

    @Resource
    private AttributeService attributeService;

    @Resource
    private CouponService couponService;

    @Resource
    private CouponUseRelationService couponUseRelationService;

    @Resource
    private BrandService brandService;

    @Resource
    private TransactionTemplate requiredTransactionTemplate;

    @Resource
    private OrderMapper orderMapper;

    @Resource
    private OrderItemServiceImpl orderItemServiceImpl;

    @Resource
    private TablePositionService tablePositionService;

    @Resource
    private TableBusinessRecordService tableBusinessRecordService;

    @Resource
    private TableOrderService tableOrderService;

    @Resource
    private OrderServiceImpl orderServiceImpl;

    @Override
    public String getOrderType() {
        return OrderType.MEAL.getType();
    }

    @Override
    public OrderConfirmVO preOrder(OrderParam param) {
        return preMealOrder(param.getProductParams(), param.getUserId(), param.getCouponId(), param.getAddressId());
    }

    /**
     * 创建订单分两种情况：
     * 1. 桌位处于清台时：
     * 1. 当前订单为第一次下单。
     * 2. 修改桌位状态为开台中。
     * 2. 桌位处于开台中时：
     * 1. 当前订单为追加订单。
     * 2.
     */
    @Override
    public OrderCreateVO createOrder(OrderParam param) {
        validCreateOrderParam(param);
        TablePosition tablePosition = getTablePosition(param.getAddressId());
        OrderEntity orderEntity = createOrderEntity(param);
        // 方法中做了校验
        PackageOrderItems packageOrderItems = packageOrderItems(orderEntity, param.getProductParams(), tablePosition);
        List<OrderItemEntity> itemEntities = packageOrderItems.getItemEntities();
        Integer hours = packageOrderItems.getHours();
        calculateOrderPrice(orderEntity, itemEntities, param);

        AtomicReference<ApiResponse<?>> response = new AtomicReference<ApiResponse<?>>(ApiResponse.failResult());

        // 获取桌位信息 桌位id对应addressId
        // 下单时最后一次开台记录
        TableBusinessRecord tableRecordLatest = getTableRecordLatest(tablePosition.getTableId());
        // 本次是否是第一次下单
        AtomicReference<Boolean> isFirstOrder = new AtomicReference<>(isFirstOrder(tableRecordLatest));

        OrderCreateVO orderCreateVO = new OrderCreateVO();
        requiredTransactionTemplate.execute(new TransactionCallbackWithoutResult() {
            @Override
            protected void doInTransactionWithoutResult(@Nonnull TransactionStatus status) {
                try {
                    // 当前桌（开台记录）未支付订单，null表示没有
                    OrderEntity unpaidOrder = null;
                    TableOrder tableOrder;
                    TableBusinessRecord tableBusinessRecord;
                    CreateMealOrderParam c = new CreateMealOrderParam();
                    if (!isFirstOrder.get()) {
                        unpaidOrder = getUnpaidOrder(tableRecordLatest.getId());
                    }
                    if (unpaidOrder == null) {
                        // 首单/非首单 创建新订单
                        // 1. 新增订单表
                        OrderEntity lock = saveAndLock(orderEntity);
                        if (lock == null) {
                            return;
                        }
                        fillOrderCreateVO(lock, orderCreateVO);
                        // 新增订单详情：
                        List<OrderItem> items = itemEntities.stream().map(item -> Convert.convert(OrderItem.class, item)).collect(Collectors.toList());
                        saveAndLockDetail(items);
                        tableBusinessRecord = packageRecordParam(tablePosition, param, hours, null);
                        tableOrder = packageTableOrderParam(tablePosition);
                        tableOrder.setTableBusinessId(tableBusinessRecord.getId());
                        tableOrder.setOrderId(lock.getOrderId()); // todo 必须放在这里
                        c.setTableOrder(tableOrder);
                        c.setOrderType(OrderSequenceTypeEnum.FIRST.getCode());
                        c.setOperation(OrderRecordOperationEnum.INSERT.getCode());
                    } else {
                        // 首单/非首单 追单
                        // 将商品追加到unpaidOrder中，并重新计算，unpaidOrder价格等
                        // 无需调用store服务
                        OrderEntity lock = orderMapper.lockById(unpaidOrder.getOrderId());
                        if (lock == null) {
                            return;
                        }
                        lock.setOrderCost(orderEntity.getOrderCost().add(lock.getOrderCost()));
                        lock.setActualPaymentAmount(orderEntity.getActualPaymentAmount().add(lock.getActualPaymentAmount()));
                        lock.setCouponAmount(orderEntity.getCouponAmount().add(lock.getCouponAmount()));
                        lock.setDiscountAmount(orderEntity.getDiscountAmount().add(lock.getDiscountAmount()));
                        lock.setUpdateTime(orderEntity.getCreateTime());
                        orderMapper.updateById(lock);
                        List<OrderItem> items = itemEntities.stream().map(item -> Convert.convert(OrderItem.class, item)).collect(Collectors.toList());
                        saveAndLockDetail(items);
                        fillOrderCreateVO(lock, orderCreateVO);
                    }
                    // 调用store服务 处理tableOrder表和tableBusinessRecord表
                    if (isFirstOrder.get()) {
                        tableBusinessRecord = packageRecordParam(tablePosition, param, hours, null);
                        c.setTableBusinessRecord(tableBusinessRecord);
                        tableBusinessRecordService.orderTableOperation(c);
                    }
                    response.set(ApiResponse.succResult());
                } catch (Exception e) {
                    status.setRollbackOnly();
                }
            }
        });
        if (!response.get().successful()) {
            throw new BizException("操作异常");
        }
        return orderCreateVO;
    }

    private boolean isFirstOrder(TableBusinessRecord tableRecordLatest) {
        return tableRecordLatest == null || tableRecordLatest.getStatus().equals(TableOpenStatus.CLOSE.getCode());
    }

    private TableOrder packageTableOrderParam(TablePosition tablePosition) {
        TableOrder result = new TableOrder();
        Date date = new Date();
        result.setId(IdGenerator.generateKey(date, tablePosition.getTableName()).toString());
        result.setTableId(tablePosition.getTableId());
        result.setCreateTime(date);
        result.setType(OrderSequenceTypeEnum.FIRST.getCode());
        return result;
    }

    private TableBusinessRecord packageRecordParam(TablePosition table, OrderParam param, Integer hours, TableBusinessRecord oldRecord) {
        TableBusinessRecord record = new TableBusinessRecord();
        Date date = new Date();
        record.setId(IdGenerator.generateKey(date, table.getTableName()).toString());
        record.setTableId(table.getTableId());
        record.setBuyHour(hours);
        record.setCreateTime(date);
        record.setUserId(param.getUserId());
        record.setStatus(TableOpenStatus.OPEN.getCode());
        record.setModifiedTime(date);
        return record;
    }

    @Override
    public boolean cancel(String orderId) {
        return false;
    }

    /**
     * 查询本次开台是否有未支付的订单
     * todo 方法内包含测试代码，上线前记得删除
     *
     * @param recordId 开台记录id
     */
    private OrderEntity getUnpaidOrder(String recordId) {
        int count = 0;
        OrderEntity o = null;
        List<TableOrder> tableOrderList = getTableOrderList(recordId);
        if (CollectionUtils.isEmpty(tableOrderList)) {
            return null;
        }
        List<String> orderIds = tableOrderList.stream().map(TableOrder::getOrderId).collect(Collectors.toList());
        List<OrderEntity> orderEntities = orderServiceImpl.listByIds(orderIds);
        for (OrderEntity entity : orderEntities) {
            if (entity.getOrderStatus() == OrderStatus.UN_PAID.getCode()) {
                count++;
                // return entity;
                o = entity;
            }
        }
        if (count > 1) {
            throw new BizException("当前桌未支付订单超过一个，系统错误");
        }
        return o;
    }

    private boolean saveAndLockDetail(List<OrderItem> orderItems) {
        return orderItemServiceImpl.addBatch(orderItems);
    }

    private void fillOrderCreateVO(OrderEntity lock, OrderCreateVO vo) {
        vo.setOrderId(lock.getOrderId());
        // 暂时没有优惠
        vo.setActualPaymentAmount(lock.getActualPaymentAmount());
    }

    private OrderEntity saveAndLock(OrderEntity orderEntity) {
        int count = orderMapper.insert(orderEntity);
        if (count != 1) {
            return null;
        }
        return orderEntity;
    }

    private void calculateOrderPrice(OrderEntity orderEntity, List<OrderItemEntity> itemEntities, OrderParam param) {
        if (CollectionUtils.isEmpty(itemEntities)) {
            throw new BizException("商品可用为空");
        }
        BigDecimal total = BigDecimal.ZERO;
        // 计算商品总价
        for (OrderItemEntity itemEntity : itemEntities) {
            total = total.add(itemEntity.getSkuTotal());
        }
        orderEntity.setOrderCost(total);
        // 计算商品优惠券价格
        if (StringUtils.isEmpty(param.getCouponId())) {
            orderEntity.setCouponAmount(BigDecimal.ZERO);
        } else {
            // 使用优惠券，校验优惠券
            getUserCoupon(param.getUserId(), param.getCouponId());
        }
        // 暂无优惠券，实付款 = total
        orderEntity.setActualPaymentAmount(total);
        orderEntity.setActualPaymentAmount(orderEntity.getOrderCost().subtract(orderEntity.getCouponAmount()));
        orderEntity.setOrderFee(BigDecimal.ZERO);
    }

    private PackageOrderItems packageOrderItems(OrderEntity orderEntity, List<ProductParam> productParams, TablePosition tablePosition) {

        Map<String, Integer> skuId2Num = productParams.stream().collect(Collectors.groupingBy(ProductParam::getSkuId, Collectors.summingInt(ProductParam::getNum)));
        // 获取商品信息
        List<Sku> skuList = getSkuList(new ArrayList<>(skuId2Num.keySet()));
        Integer hours = validOrder(tablePosition, skuList, productParams);
        // 判断商品是否下架、删除、商品不存在情况
        Map<String, Sku> skuMap = new HashMap<>();
        Set<String> itemIds = new HashSet<>();
        Set<Long> storeIds = new HashSet<>();
        Set<Long> brandIds = new HashSet<>();
        if (skuList.size() != skuId2Num.size()) {
            throw new BizException("商品不存在");
        }
        Set<String> attributeCode = new HashSet<>();
        Set<String> attributeValueCode = new HashSet<>();
        for (Sku sku : skuList) {
            if (sku.getShelvesStatus() != ItemShelvesStatus.ON.getStatus()) {
                throw new BizException("商品已下架");
            }
            AttributeUtils.parseAttributeAndValue(sku.getAttributeSet(), attributeCode, attributeValueCode);
            skuMap.put(sku.getSkuId(), sku);
            itemIds.add(sku.getItemId());
        }
        // 商品spu信息
        List<ItemBasic> itemBasicList = getItemBasic(new ArrayList<>(itemIds));
        if (itemBasicList.size() != itemIds.size()) {
            throw new BizException("商品不存在");
        }
        Map<String, ItemBasic> itemBasicMap = new HashMap<>();
        for (ItemBasic itemBasic : itemBasicList) {
            if (itemBasic.getShelvesStatus() != ItemShelvesStatus.ON.getStatus()) {
                throw new BizException("商品已下架");
            }
            if (!Objects.equals(itemBasic.getAttribution(), CategoryAttributionEnum.TYPE_ORDER_FOOD.getType())) {
                throw new BizException("包含非点餐商品");
            }
            itemBasicMap.put(itemBasic.getItemId(), itemBasic);
            storeIds.add(itemBasic.getStoreId());
            brandIds.add(itemBasic.getBrandId());
        }
        List<Attribute> attributes = getAttributeList(attributeCode);
        List<AttributeValue> attributeValues = getAttributeValueList(attributeCode, attributeValueCode);
        Map<String, Attribute> attributeMap = attributes.stream().collect(Collectors.toMap(Attribute::getAttributeCode, Function.identity()));
        Map<String, AttributeValue> attributeValueMap = attributeValues.stream().collect(Collectors.toMap(item -> item.getAttributeCode() + "_" + item.getValueCode(), Function.identity()));
        List<StoreInfo> storeInfos = getStoreList(new ArrayList<>(storeIds));
        Map<Long, StoreInfo> storeId2Info = storeInfos.stream().collect(Collectors.toMap(StoreInfo::getStoreId, Function.identity()));
        List<Brand> brandList = getBrandList(brandIds);
        Map<Long, Brand> id2Brand = brandList.stream().collect(Collectors.toMap(Brand::getBrandId, Function.identity()));
        List<OrderItemEntity> orderItemEntities = new ArrayList<>();
        Date date = new Date();
        skuId2Num.forEach((skuId, num) -> {
            OrderItemEntity orderItemEntity = new OrderItemEntity();
            orderItemEntity.setId(IdGenerator.generateKey(date, orderEntity.getOrderId()).toString());
            orderItemEntity.setOrderId(orderEntity.getOrderId());
            // 必存在
            Sku sku = skuMap.get(skuId);
            // 必存在
            ItemBasic itemBasic = itemBasicMap.get(sku.getItemId());
            orderItemEntity.setItemId(itemBasic.getItemId());
            orderItemEntity.setItemCode(itemBasic.getItemCode());
            orderItemEntity.setItemName(itemBasic.getItemName());
            orderItemEntity.setSkuId(sku.getSkuId());
            orderItemEntity.setSkuCode(sku.getSkuCode());
            orderItemEntity.setSkuNum(num);
            orderItemEntity.setSkuPicture(sku.getPictureSet());
            orderItemEntity.setSkuPrice(sku.getSellPrice());
            orderItemEntity.setSkuTotal(sku.getSellPrice().multiply(new BigDecimal(num)));
            orderItemEntity.setSkuName(AttributeUtils.parseSkuName(sku.getAttributeSet(), attributeMap, attributeValueMap));
            orderItemEntity.setSkuAttrs(AttributeUtils.parseSkuAttrs(sku.getAttributeSet(), attributeMap, attributeValueMap));
            orderItemEntity.setDiscountAmount(BigDecimal.ZERO);
            StoreInfo storeInfo = storeId2Info.get(itemBasic.getStoreId());
            if (storeInfo != null) {
                orderItemEntity.setStoreId(storeInfo.getStoreId());
                orderItemEntity.setStoreName(storeInfo.getStoreName());
            }
            orderItemEntity.setCategoryCode(itemBasic.getCategoryThirdId() + "");
            Brand brand = id2Brand.get(itemBasic.getBrandId());
            if (brand != null) {
                orderItemEntity.setBrandId(brand.getBrandId());
                orderItemEntity.setBrandName(brand.getBrandCnName());
            }
            orderItemEntities.add(orderItemEntity);
        });
        PackageOrderItems result = new PackageOrderItems();
        result.setItemEntities(orderItemEntities);
        result.setHours(hours);
        return result;
    }

    private List<Brand> getBrandList(Set<Long> brandIds) {
        if (CollectionUtils.isEmpty(brandIds)) {
            return Collections.emptyList();
        }
        return brandService.listByIds(brandIds);
    }

    private OrderEntity createOrderEntity(OrderParam param) {
        Date date = new Date();
        OrderEntity orderEntity = new OrderEntity();
        orderEntity.setOrderId(IdGenerator.generateKey(date, param.getUserId()).toString());
        // orderEntity.setUserId(param.getUserId());
        orderEntity.setOrderType(OrderType.MEAL.getType());
        ClientAppTypeEnum clientAppTypeEnum = SessionLocalUtils.getClientAppType();
        if (clientAppTypeEnum != null) {
            orderEntity.setOrderSource(clientAppTypeEnum.getCode());
        } else {
            orderEntity.setOrderSource(ClientAppTypeEnum.WEB.getCode());
        }
        orderEntity.setStoreId(param.getStoreId());
        orderEntity.setParentId("0");
        orderEntity.setOrderStatus(OrderStatus.UN_PAID.getCode());
        orderEntity.setBuyerMessage(param.getBuyerMessage());
        orderEntity.setShippingMethod(DeliveryMethodEnum.NONE.getCode());
        orderEntity.setOrderFee(BigDecimal.ZERO);
        orderEntity.setDeleteStatus(OrderDeleteEnum.NO.getCode());
        orderEntity.setCreateTime(date);
        orderEntity.setUpdateTime(date);
        return orderEntity;
    }

    private void validCreateOrderParam(OrderParam param) {
        if (CollectionUtils.isEmpty(param.getProductParams())) {
            throw new BizException("商品信息为空");
        }
        for (ProductParam productParam : param.getProductParams()) {
            if (productParam.getNum() == null || productParam.getNum() <= 0) {
                throw new BizException("商品数量不合法");
            }
            if (StringUtils.isEmpty(productParam.getSkuId())) {
                throw new BizException("商品不能为空");
            }
        }
        if (param.getStoreId() == null) {
            throw new BizException("店铺不能为空");
        }
    }

    private OrderConfirmVO preMealOrder(List<ProductParam> productParams, String userId, String couponId, String addressId) {
        // 校验商品参数
        validProductParam(productParams);
        // 获取商品数据
        List<String> skuIds = productParams.stream().map(ProductParam::getSkuId).collect(Collectors.toList());
        List<Sku> skuList = getSkuList(skuIds);
        // 获取优惠券信息
        List<UserCoupon> coupons = getUserCoupon(userId, couponId);
        List<String> itemIds = skuList.stream().map(Sku::getItemId).collect(Collectors.toList());
        // 商品其他信息
        // 获取商品
        // 获取商品信息
        List<ItemBasic> itemBasics = getItemBasic(itemIds);

        // 获取桌位信息 桌位id对应addressId
        TablePosition tablePosition = getTablePosition(addressId);

        // 桌位对应的套餐商品spu id和桌位商品spu id
        validOrder(tablePosition, skuList, productParams);

        return packagePreMealOrder(coupons, itemBasics, skuList, productParams);
    }

    private Map<Integer, List<String>> getComboAndTableItems(Long tableId) {
        Map<Integer, List<String>> comboAndTableItems = itemBasicService.getTableItem(tableId);
        if (comboAndTableItems.size() < 2) {
            throw new BizException("桌位绑定信息少于两种");
        }
        if (comboAndTableItems.containsKey(CategoryTypeEnum.COMBO.getType())) {
            if (CollectionUtils.isEmpty(comboAndTableItems.get(CategoryTypeEnum.COMBO.getType()))) {
                throw new BizException("桌位未绑定套餐商品");
            }
            if (CollectionUtils.isEmpty(comboAndTableItems.get(CategoryTypeEnum.TABLE.getType()))) {
                throw new BizException("桌位未绑定桌位费用商品");
            }
        }
        return comboAndTableItems;
    }

    private List<OrderItem> getOrderItemList(List<String> orderIds) {
        if (CollectionUtils.isEmpty(orderIds)) {
            return Collections.emptyList();
        }
        return orderItemServiceImpl.listByOrderIds(new HashSet<>(orderIds));
    }

    private List<OrderItem> getOrderItemList(String orderId) {
        if (StrUtil.isEmpty(orderId)) {
            return Collections.emptyList();
        }
        return orderItemServiceImpl.findOrderItemByOrder(orderId);
    }

    private List<Order> getOrderList(TableBusinessRecord tableRecord) {
        if (tableRecord == null) {
            return Collections.emptyList();
        }
        if (tableRecord.getStatus().equals(TableOpenStatus.CLOSE.getCode())) {
            return Collections.emptyList();
        }
        // 当前桌位处于开台中，一定有历史订单，tableOrders必不为空
        List<TableOrder> tableOrders = getTableOrderList(tableRecord.getId());

        List<String> orderIds = tableOrders.stream().map(TableOrder::getOrderId).collect(Collectors.toList());

        return getOrderList(orderIds);
    }

    private List<Order> getOrderList(List<String> orderIds) {
        if (CollectionUtils.isEmpty(orderIds)) {
            return Collections.emptyList();
        }
        return orderServiceImpl.listByIds(orderIds).stream().map(item -> Convert.convert(Order.class, item)).collect(Collectors.toList());
    }

    private List<TableOrder> getTableOrderList(String tableRecordId) {
        if (StrUtil.isEmpty(tableRecordId)) {
            return Collections.emptyList();
        }
        List<TableOrder> tableOrders = tableOrderService.listByTableBusinessRecordId(tableRecordId);
        if (CollectionUtils.isEmpty(tableOrders)) {
            return Collections.emptyList();
        }
        return tableOrders;
    }

    private TableBusinessRecord getTableRecordLatest(Long tableId) {
        if (tableId == null) {
            throw new BizException("桌位不存在");
        }
        return tableBusinessRecordService.getLatestOneByTableId(tableId);
    }

    private TablePosition getTablePosition(String addressId) {
        long tableId = Long.parseLong(addressId);
        TablePosition table = tablePositionService.findByTableId(tableId);
        if (table == null || table.getDeleteStatus().equals(TableDeleteStatus.DELETED.getValue()) ||
                table.getEnableStatus().equals(TableEnableStatus.DISABLE.getValue())) {
            throw new BizException("桌位不存在");
        }
        return table;
    }

    private OrderConfirmVO packagePreMealOrder(List<UserCoupon> coupons, List<ItemBasic> itemBasics, List<Sku> skuList, List<ProductParam> productParams) {
        OrderConfirmVO orderConfirmVO = new OrderConfirmVO();
        // 组装商品信息，商品价格
        orderConfirmVO.setProducts(packageProduct(itemBasics, skuList, productParams));
        // 商品总价
        orderConfirmVO.setTotalPrice(calculateSkuPrice(skuList, productParams));
        // 组装优惠券信息
        packageCoupon(coupons, orderConfirmVO, itemBasics);
        if (orderConfirmVO.getCouponPaymentAmount() == null) {
            orderConfirmVO.setCouponPaymentAmount(BigDecimal.ZERO);
        }
        orderConfirmVO.setFreight(BigDecimal.ZERO);
        orderConfirmVO.setActualPaymentAmount(orderConfirmVO.getTotalPrice().subtract(orderConfirmVO.getCouponPaymentAmount()).add(orderConfirmVO.getFreight()));
        return orderConfirmVO;
    }

    /**
     * 下单校验。
     * <p>
     * 规则：
     * 散台不做任何校验，直接返回当前订单下单时长。以下规则仅针对包间：
     * <p>
     * 1.首单必选桌位费或者套餐，必须二选一。
     * <p>
     * 2.从开台开始的所有订单，如果首单下的是桌位费/套餐，那么追加商品也只能包含对应的桌位费或桌位费续费商品/套餐或套餐续费商品。
     *
     * @param tablePosition 桌位信息
     * @param skuList       待创建订单的sku集合
     * @param productParams 待创建订单的skuId和对应数量集合
     * @return 开台时长或者续费时长
     */
    private Integer validOrder(TablePosition tablePosition, List<Sku> skuList, List<ProductParam> productParams) {
        int hours = 0;
        // 获取当前桌位最后一次服务记录
        TableBusinessRecord tableRecordLatest = getTableRecordLatest(tablePosition.getTableId());
        // 散台操作
        if (tablePosition.getCategory().equals(TableCategoryEnum.HALL.getValue())) {
            // 首单
            if (isFirstOrder(tableRecordLatest)) {
                return MealHoursEnum.HALL_OPEN.getHours();
            } else {
                String renewSkuId = tablePosition.getRenewalNormalProductId();
                Map<String, Integer> map = productParams.stream().collect(Collectors.toMap(ProductParam::getSkuId, ProductParam::getNum));
                if (map.containsKey(renewSkuId)) {
                    return map.get(renewSkuId) * MealHoursEnum.HALL_RENEW.getHours();
                }
                return hours;
            }
        }

        Map<Integer, List<String>> comboAndTableItems = getComboAndTableItems(tablePosition.getTableId());

        List<String> comboItemIds = getItemId(comboAndTableItems, CategoryTypeEnum.COMBO.getType());
        List<String> tableItemIds = getItemId(comboAndTableItems, CategoryTypeEnum.TABLE.getType());
        Set<String> comboItemIdsSet = new HashSet<>(comboItemIds);
        Set<String> tableItemIdsSet = new HashSet<>(tableItemIds);
        int comboNum = 0;
        int tableNum = 0;
        Map<String, Integer> itemId2Num = productParams2Items(productParams, skuList);
        // 包间 & 首单
        if (isFirstOrder(tableRecordLatest)) {
            // 校验互斥且必选
            for (Map.Entry<String, Integer> entry : itemId2Num.entrySet()) {
                if (comboItemIdsSet.contains(entry.getKey())) {
                    comboNum++;
                }
                if (tableItemIdsSet.contains(entry.getKey())) {
                    tableNum++;
                }
            }
            if (tableNum > 0 && comboNum > 0) {
                throw new BizException("套餐商品与时长商品只能选择一种");
            }
            if (tableNum == 0 && comboNum == 0) {
                throw new BizException("套餐商品与时长商品必须选择其中一种");
            }
            if (tableNum != 0) {
                return tableNum * MealHoursEnum.ROOM_OPEN.getHours();
            }
            return comboNum * MealHoursEnum.COMBO_OPEN.getHours();
        } else {
            // 获取首单商品，校验互斥
            TableOrder firstTableOrder = getFirstOrder(tableRecordLatest.getId());
            if (firstTableOrder == null) {
                throw new BizException("数据异常");
            }
            boolean isCombo = false;
            boolean isTable = false;
            int comboRenewNum = 0;
            int tableRenewNum = 0;
            List<OrderItem> orderItems = getOrderItemList(firstTableOrder.getOrderId());
            Map<String, Integer> itemId2NumOld = productParams2Items(orderItems);
            for (Map.Entry<String, Integer> entry : itemId2NumOld.entrySet()) {
                if (comboItemIdsSet.contains(entry.getKey())) {
                    isCombo = true;
                    break;
                }
                if (tableItemIdsSet.contains(entry.getKey())) {
                    isTable = true;
                    break;
                }
            }
            for (Map.Entry<String, Integer> entry : itemId2Num.entrySet()) {
                if (comboItemIdsSet.contains(entry.getKey())) {
                    comboNum++;
                }
                if (tableItemIdsSet.contains(entry.getKey())) {
                    tableNum++;
                }
            }
            for (ProductParam param : productParams) {
                if (Objects.equals(param.getSkuId(), tablePosition.getRenewalComboProductId())) {
                    comboRenewNum += param.getNum();
                }
                if (Objects.equals(param.getSkuId(), tablePosition.getRenewalNormalProductId())) {
                    tableRenewNum += param.getNum();
                }
            }
            if (isCombo) {
                if (tableNum + tableRenewNum > 0) {
                    throw new BizException("商品类型冲突");
                }
                hours += comboNum * MealHoursEnum.COMBO_OPEN.getHours() + comboRenewNum * MealHoursEnum.COMBO_RENEW.getHours();
            }
            if (isTable) {
                if (comboNum + comboRenewNum > 0) {
                    throw new BizException("商品类型冲突");
                }
                hours += tableNum * MealHoursEnum.ROOM_OPEN.getHours() + tableRenewNum * MealHoursEnum.ROOM_RENEW.getHours();
            }
            return hours;
        }
    }

    /**
     * 根据订单项列表将订单项转换为商品ID和对应数量的Map。
     *
     * @param orderItems 订单项列表
     * @return itemId -> 相同itemId对应商品数量的和
     */
    private Map<String, Integer> productParams2Items(List<OrderItem> orderItems) {
        return orderItems.stream().collect(Collectors.groupingBy(OrderItem::getItemId, Collectors.summingInt(OrderItem::getSkuNum)));
    }

    private TableOrder getFirstOrder(String recordId) {
        if (StrUtil.isEmpty(recordId)) {
            return null;
        }
        List<TableOrder> tableOrders = tableOrderService.listByTableBusinessRecordIdAndType(recordId, OrderSequenceTypeEnum.FIRST.getCode());
        if (CollectionUtils.isEmpty(tableOrders)) {
            return null;
        }
        if (tableOrders.size() == 1) {
            return tableOrders.get(0);
        }
        throw new BizException("数据异常");
    }

    /**
     * 将 {skuId+num} 转换成 {itemId+num}
     *
     * @param productParams skuId + num
     * @param skuList       sku
     * @return itemId -> num
     */
    private Map<String, Integer> productParams2Items(List<ProductParam> productParams, List<Sku> skuList) {
        Map<String, Integer> itemId2Num = new HashMap<>();
        Map<String, String> skuId2ItemId = skuList.stream().collect(Collectors.toMap(Sku::getSkuId, Sku::getItemId));
        for (ProductParam param : productParams) {
            String itemId = skuId2ItemId.get(param.getSkuId());
            if (StrUtil.isNotEmpty(itemId)) {
                itemId2Num.put(itemId, itemId2Num.getOrDefault(itemId, 0) + param.getNum());
            }
        }
        return itemId2Num;
    }

    /**
     * 检查待创建订单中是否有套餐商品或者桌位商品，同时，如果套餐商品数量大于1抛出异常
     *
     * @param itemIds      待创建订单商品 Key(itemId) Value(数量) 的Map集合
     * @param comboItemIds 当前桌位套餐商品itemId集合
     * @param tableItemIds 当前桌位桌位商品itemId集合
     * @return true包含，false不包含
     */
    private boolean hasComboOrTable(Map<String, Integer> itemIds, List<String> comboItemIds, List<String> tableItemIds) {
        int comboNum = 0;
        boolean has = false;
        boolean combo = false;
        boolean table = false;
        Set<String> comboItemIdSet = new HashSet<>(comboItemIds);
        Set<String> tableItemIdSet = new HashSet<>(tableItemIds);
        for (Map.Entry<String, Integer> entry : itemIds.entrySet()) {
            if (comboItemIdSet.contains(entry.getKey())) {
                combo = true;
                comboNum += entry.getValue();
                if (comboNum > 1) {
                    throw new BizException("套餐商品数量不能大于1");
                }
                has = true;
            }
            if (tableItemIdSet.contains(entry.getKey())) {
                table = true;
                has = true;
            }
            if (table && combo) {
                throw new BizException("套餐与桌位费用只能选择一个");
            }
        }
        return has;
    }

    private List<String> getItemId(Map<Integer, List<String>> comboAndTableItems, Integer type) {
        if (!comboAndTableItems.containsKey(type)) {
            throw new BizException("桌位绑定商品有误");
        }
        return comboAndTableItems.get(type);
    }

    private void packageCoupon(List<UserCoupon> coupons, OrderConfirmVO orderConfirmVO, List<ItemBasic> itemBasics) {
        Date date = new Date();
        if (!CollectionUtils.isEmpty(coupons)) {
            UserCoupon userCoupon = null;
            for (UserCoupon coupon : coupons) {
                if (UserCouponUseStatus.UN_USE.getStatus() == coupon.getUseStatus()
                        && UserCouponDeleteStatus.NORMAL.getStatus() == coupon.getCouponDelete() && date.compareTo(coupon.getExpireTime()) < 0) {
                    userCoupon = coupon;
                    break;
                }
            }
            boolean couponValid = false;
            if (userCoupon != null) {
                Coupon coupon = couponService.detail(userCoupon.getCouponId());
                if (coupon != null) {
                    // 校验优惠券类型是否满足商品或店铺
                    CouponUseType couponUseType = CouponUseType.getEnum(coupon.getUseType());
                    List<CouponUseRelation> couponUseRelations = null;
                    if (couponUseType == null) {
                        couponValid = false;
                    } else {
                        switch (couponUseType) {
                            case ALL:
                                couponValid = true;
                                break;
                            case STORE:
                                couponUseRelations = couponUseRelationService.list(coupon.getId());
                                if (CollectionUtils.isEmpty(couponUseRelations)) {
                                    List<Long> storeId = itemBasics.stream().map(ItemBasic::getStoreId).collect(Collectors.toList());
                                    List<Long> couponStoreIds = couponUseRelations.stream().map(item -> Long.parseLong(item.getUseId())).collect(Collectors.toList());
                                    for (Long couponStoreId : couponStoreIds) {
                                        if (storeId.contains(couponStoreId)) {
                                            couponValid = true;
                                            break;
                                        }
                                    }
                                }
                                break;
                            case PRODUCT:
                                couponUseRelations = couponUseRelationService.list(coupon.getId());
                                if (!CollectionUtils.isEmpty(couponUseRelations)) {
                                    List<String> itemIds = itemBasics.stream().map(ItemBasic::getItemId).collect(Collectors.toList());
                                    List<String> couponItemIds = couponUseRelations.stream().map(CouponUseRelation::getUseId).collect(Collectors.toList());
                                    for (String couponItemId : couponItemIds) {
                                        if (itemIds.contains(couponItemId)) {
                                            couponValid = true;
                                            break;
                                        }
                                    }
                                }
                                break;
                            case CATEGORY:
                                couponUseRelations = couponUseRelationService.list(coupon.getId());
                                if (!CollectionUtils.isEmpty(couponUseRelations)) {
                                    Set<String> categoryIds = new HashSet<>();
                                    for (ItemBasic itemBasic : itemBasics) {
                                        categoryIds.add(itemBasic.getCategoryFirstId().toString());
                                        categoryIds.add(itemBasic.getCategorySecondId().toString());
                                        categoryIds.add(itemBasic.getCategoryThirdId().toString());
                                    }
                                    for (CouponUseRelation couponUseRelation : couponUseRelations) {
                                        if (categoryIds.contains(couponUseRelation.getUseId())) {
                                            couponValid = true;
                                            break;
                                        }
                                    }
                                }
                                break;
                            default:
                                break;
                        }
                    }
                    if (couponValid) {
                        // 优惠券有效，校验优惠券使用规则
                        boolean userRuleValid = false;
                        if (StringUtils.isNotEmpty(coupon.getUseRule())) {
                            CouponUserRule couponUserRole = JSONUtil.toBean(coupon.getUseRule(), CouponUserRule.class);
                            CouponUseRuleType couponUseRuleType = CouponUseRuleType.getEnum(couponUserRole.getType());
                            if (couponUseRuleType == null) {
                                userRuleValid = true;
                            } else {
                                switch (couponUseRuleType) {
                                    case NO_THRESHOLD:
                                        if (orderConfirmVO.getTotalPrice().compareTo(coupon.getMoney()) > 0) {
                                            userRuleValid = true;
                                        }
                                        break;
                                    case ORDER_AMOUNT:
                                        if (orderConfirmVO.getTotalPrice().compareTo(coupon.getMoney()) > 0
                                                && couponUserRole.getOrderAmount().compareTo(orderConfirmVO.getTotalPrice()) <= 0) {
                                            userRuleValid = true;
                                        }
                                        break;
                                    default:
                                        break;
                                }
                            }
                        }
                        if (userRuleValid) {
                            // 拼装优惠券内容
                            orderConfirmVO.setCouponPaymentAmount(coupon.getMoney());
                            CouponVO couponVO = new CouponVO();
                            couponVO.setCouponId(coupon.getId());
                            couponVO.setCouponCode(coupon.getCouponCode());
                            couponVO.setCouponName(coupon.getCouponName());
                            couponVO.setCouponMoney(coupon.getMoney());
                            orderConfirmVO.setCoupon(couponVO);
                        }
                    }
                }
            }
        }
    }

    private BigDecimal calculateSkuPrice(List<Sku> skuList, List<ProductParam> productParams) {
        BigDecimal amount = BigDecimal.ZERO;
        Map<String, Sku> map = skuList.stream().collect(Collectors.toMap(Sku::getSkuId, item -> item));
        for (ProductParam productParam : productParams) {
            if (map.containsKey(productParam.getSkuId())) {
                Sku sku = map.get(productParam.getSkuId());
                amount = amount.add(sku.getSellPrice().multiply(new BigDecimal(productParam.getNum())));
            }
        }
        return amount;
    }

    private List<ProductOrderVO> packageProduct(List<ItemBasic> itemBasics, List<Sku> skuList, List<ProductParam> productParams) {
        List<Long> storeIds = new ArrayList<>();
        Map<String, ItemBasic> itemId2Basic = new HashMap<>();
        for (ItemBasic itemBasic : itemBasics) {
            storeIds.add(itemBasic.getStoreId());
            itemId2Basic.put(itemBasic.getItemId(), itemBasic);
            if (!itemBasic.getAttribution().equals(CategoryAttributionEnum.TYPE_ORDER_FOOD.getType())) {
                throw new BizException("当前订单存在非点餐商品");
            }
        }
        // 查询店铺信息
        List<StoreInfo> storeInfos = getStoreList(storeIds);
        Map<Long, StoreInfo> storeId2Info = storeInfos.stream().collect(Collectors.toMap(StoreInfo::getStoreId, Function.identity()));
        List<ProductOrderVO> productOrderVOS = new ArrayList<>();
        Map<String, Sku> skuMap = skuList.stream().collect(Collectors.toMap(Sku::getSkuId, Function.identity()));
        // 查询属性和属性值
        Set<String> attributeCode = new HashSet<>();
        Set<String> attributeValueCode = new HashSet<>();
        for (ProductParam productParam : productParams) {
            Sku sku = skuMap.get(productParam.getSkuId());
            AttributeUtils.parseAttributeAndValue(sku.getAttributeSet(), attributeCode, attributeValueCode);
        }
        List<Attribute> attributes = getAttributeList(attributeCode);
        List<AttributeValue> attributeValues = getAttributeValueList(attributeCode, attributeValueCode);
        Map<String, Attribute> attributeMap = attributes.stream().collect(Collectors.toMap(Attribute::getAttributeCode, Function.identity()));
        Map<String, AttributeValue> attributeValueMap = attributeValues.stream().collect(Collectors.toMap(item -> item.getAttributeCode() + "_" + item.getValueCode(), Function.identity()));
        for (ProductParam productParam : productParams) {
            Sku sku = skuMap.get(productParam.getSkuId());
            ProductOrderVO vo = new ProductOrderVO();
            vo.setSkuId(sku.getSkuId());
            if (itemId2Basic.containsKey(sku.getItemId())) {
                ItemBasic itemBasic = itemId2Basic.get(sku.getItemId());
                vo.setSkuName(AttributeUtils.parseSkuName(sku.getAttributeSet(), attributeMap, attributeValueMap));
                vo.setSkuTitle(itemBasic.getItemName());
            }
            vo.setSkuPicture(sku.getPictureSet());
            vo.setSkuPrice(sku.getSellPrice());
            vo.setSkuNum(productParam.getNum());
            vo.setSkuTotalPrice(sku.getSellPrice().multiply(new BigDecimal(productParam.getNum())));
            if (ItemShelvesStatus.ON.getStatus() != sku.getShelvesStatus()) {
                vo.setSkuStatus(1);
            }
            if (itemId2Basic.containsKey(sku.getItemId())) {
                if (itemId2Basic.containsKey(sku.getItemId())) {
                    if (itemId2Basic.get(sku.getItemId()).getSellOff().equals(ItemSellOffEnum.SELL_OFF.getCode())) {
                        vo.setSkuStatus(2);
                    }
                }
            }
            if (vo.getSkuStatus() == null) {
                vo.setSkuStatus(0);
            }
            if (itemId2Basic.containsKey(sku.getItemId())) {
                StoreInfo storeInfo = storeId2Info.get(itemId2Basic.get(sku.getItemId()).getStoreId());
                vo.setStoreId(storeInfo.getStoreId());
                vo.setStoreName(storeInfo.getStoreName());
            }
            productOrderVOS.add(vo);
        }
        return productOrderVOS;
    }

    private List<AttributeValue> getAttributeValueList(Set<String> attributeCode, Set<String> attributeValueCode) {
        AttributeValueParam attributeValueParam = new AttributeValueParam();
        attributeValueParam.setAttributeCodes(attributeCode);
        attributeValueParam.setValueCodes(attributeValueCode);
        return attributeValueService.listByValueCodeAndAttrCode(attributeValueParam);
    }

    private List<Attribute> getAttributeList(Set<String> attributeCode) {
        if (CollectionUtils.isEmpty(attributeCode)) {
            return Collections.emptyList();
        }
        return attributeService.listByAttrCodes(attributeCode, null, null);
    }

    private List<StoreInfo> getStoreList(List<Long> storeIds) {
        if (CollectionUtils.isEmpty(storeIds)) {
            return Collections.emptyList();
        }
        return storeInfoService.variableList(storeIds);
    }

    private List<SkuInventory> getSkuInventoryList(List<String> skuIds) {
        if (CollectionUtils.isEmpty(skuIds)) {
            return Collections.emptyList();
        }
        return skuInventoryService.listBySkuIds(new HashSet<>(skuIds), SkuInventoryDeleteEnum.NORMAL);
    }

    private List<ItemBasic> getItemBasic(List<String> itemIds) {
        if (CollectionUtils.isEmpty(itemIds)) {
            return Collections.emptyList();
        }
        return itemBasicService.listBatch(new HashSet<>(itemIds), ItemDeleteEnum.NORMAL, null);
    }

    private List<ItemOther> getItemOtherList(List<String> itemIds) {
        if (CollectionUtils.isEmpty(itemIds)) {
            return Collections.emptyList();
        }
        return itemOtherService.listByItemIds(new HashSet<>(itemIds));
    }

    private List<UserCoupon> getUserCoupon(String userId, String couponId) {
        if (StringUtils.isEmpty(userId) || StringUtils.isEmpty(couponId)) {
            return Collections.emptyList();
        }
        return userCouponService.userCouponListByCouponId(userId, couponId);
    }

    private List<Sku> getSkuList(List<String> skuIds) {
        if (CollectionUtils.isEmpty(skuIds)) {
            return Collections.emptyList();
        }
        try {
            return skuService.list(new HashSet<>(skuIds), null, SkuDeleteEnum.NORMAL, null);
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }

    private void validProductParam(List<ProductParam> productParams) {
        if (CollectionUtils.isEmpty(productParams)) {
            throw new BizException("商品参数不能为空");
        }
        for (ProductParam productParam : productParams) {
            if (StringUtils.isEmpty(productParam.getSkuId())) {
                throw new BizException("商品参数错误");
            }
            if (productParam.getNum() == null || productParam.getNum() <= 0) {
                throw new BizException("商品参数错误");
            }
        }
    }
}
