package vsphere_dim

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/vmware/govmomi"
	"github.com/vmware/govmomi/session"
	"github.com/vmware/govmomi/view"
	"github.com/vmware/govmomi/vim25"
	"github.com/vmware/govmomi/vim25/soap"
)

// Client 表示一个 vSphere 连接客户端
type Client struct {
	*govmomi.Client
	Views *view.Manager
	Root  *view.ContainerView
}

// NewClient 创建一个新的 vSphere 客户端
func NewClient(ctx context.Context, vSphereURL *url.URL, username, password string,
	timeout time.Duration, insecureSkipVerify bool) (*Client, error) {
	// 设置超时
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// 创建 SOAP 客户端
	soapClient := soap.NewClient(vSphereURL, insecureSkipVerify)

	// 创建 vim25 客户端
	vimClient, err := vim25.NewClient(ctx, soapClient)
	if err != nil {
		return nil, fmt.Errorf("failed to create vim25 client: %v", err)
	}

	// 创建 session manager
	sm := session.NewManager(vimClient)

	// 创建 govmomi 客户端
	client := &govmomi.Client{
		Client:         vimClient,
		SessionManager: sm,
	}

	// 登录 vCenter
	if username != "" && password != "" {
		if err := client.Login(ctx, url.UserPassword(username, password)); err != nil {
			return nil, fmt.Errorf("failed to login: %v", err)
		}
	}

	// 创建视图管理器
	m := view.NewManager(client.Client)

	// 创建根视图
	v, err := m.CreateContainerView(ctx, client.ServiceContent.RootFolder, []string{}, true)
	if err != nil {
		return nil, fmt.Errorf("failed to create container view: %v", err)
	}

	return &Client{
		Client: client,
		Views:  m,
		Root:   v,
	}, nil
}

// Close 关闭客户端连接
func (c *Client) Close(ctx context.Context) error {
	if c.Client != nil {
		return c.Client.Logout(ctx)
	}
	return nil
}
